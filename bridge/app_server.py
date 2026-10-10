"""Async JSON-RPC client for ``codex app-server`` stdio transport."""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import json
import logging
import os
from collections import deque
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .errors import AppServerExited, ProtocolError, RpcError

LOGGER = logging.getLogger(__name__)
JsonObject = dict[str, Any]
MAX_APP_SERVER_LINE_BYTES = 4 * 1024 * 1024
MAX_RETIRED_SERVER_RESPONSE_IDS = 1024

_MODERN_APPROVAL_METHODS = {
    "item/commandExecution/requestApproval",
    "item/fileChange/requestApproval",
}
_LEGACY_APPROVAL_METHODS = {"applyPatchApproval", "execCommandApproval"}
_OTHER_CLOSED_METHODS = {
    "item/permissions/requestApproval",
    "item/tool/requestUserInput",
    "mcpServer/elicitation/request",
    "account/chatgptAuthTokens/refresh",
    "attestation/generate",
}


class RpcSubscription:
    """Bounded fan-out queue for app-server notifications and requests."""

    def __init__(self, owner: CodexAppServer, maxsize: int = 512) -> None:
        self._owner = owner
        self._closed = False
        self.queue: asyncio.Queue[JsonObject] = asyncio.Queue(maxsize=maxsize)

    async def get(self, timeout: float | None = None) -> JsonObject:
        if timeout is None:
            return await self.queue.get()
        return await asyncio.wait_for(self.queue.get(), timeout)

    def get_nowait(self) -> JsonObject:
        """Return one already-buffered event without yielding."""
        return self.queue.get_nowait()

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._owner.unsubscribe(self)

    async def __aenter__(self) -> RpcSubscription:
        return self

    async def __aexit__(self, *_: object) -> None:
        self.close()


class CodexAppServer:
    """Own one initialized app-server process and multiplex its JSON-RPC stream."""

    def __init__(
        self,
        command: Sequence[str],
        *,
        cwd: str | Path | None = None,
        env: Mapping[str, str] | None = None,
        inherit_env: bool = True,
        request_timeout: float = 90.0,
        tool_timeout: float = 120.0,
    ) -> None:
        self.command = tuple(command)
        self.cwd = str(cwd) if cwd is not None else None
        self.env = dict(env) if env is not None else None
        self.inherit_env = inherit_env
        self.request_timeout = request_timeout
        self.tool_timeout = tool_timeout
        self.process: asyncio.subprocess.Process | None = None
        self._ids = itertools.count(1)
        self._pending: dict[int, asyncio.Future[Any]] = {}
        self._server_request_timers: dict[int | str, asyncio.Task[None]] = {}
        self._server_response_states: dict[int | str, _ServerResponseState] = {}
        self._retired_server_response_ids: dict[int | str, None] = {}
        self._subscriptions: set[RpcSubscription] = set()
        self._write_lock = asyncio.Lock()
        self._lifecycle_lock = asyncio.Lock()
        self._reader_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._closed = False
        self._initialized = False
        self._last_stderr: deque[str] = deque(maxlen=20)
        self._auth_mode: str | None = None
        self._plan_type: str | None = None

    @property
    def is_running(self) -> bool:
        return (
            self.process is not None
            and self.process.returncode is None
            and self._initialized
        )

    def health(self) -> JsonObject:
        process = self.process
        return {
            "running": self.is_running,
            "initialized": self._initialized,
            "pid": process.pid
            if process is not None and process.returncode is None
            else None,
            "returncode": process.returncode if process is not None else None,
            "auth_mode": self._auth_mode,
            "plan_type": self._plan_type,
        }

    async def start(self) -> None:
        async with self._lifecycle_lock:
            if self.is_running:
                return
            if self._closed:
                raise AppServerExited("app-server client is closed")
            if not self.command:
                raise ValueError("app-server command must not be empty")
            child_env = os.environ.copy() if self.inherit_env else {}
            if self.env is not None:
                child_env.update(self.env)
            self.process = await asyncio.create_subprocess_exec(
                *self.command,
                cwd=self.cwd,
                env=child_env,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=MAX_APP_SERVER_LINE_BYTES,
            )
            self._reader_task = asyncio.create_task(
                self._read_stdout(), name="codex-app-server-reader"
            )
            self._stderr_task = asyncio.create_task(
                self._read_stderr(), name="codex-app-server-stderr"
            )
            try:
                await self.call(
                    "initialize",
                    {
                        "clientInfo": {
                            "name": "ha_codex_voice",
                            "title": "Home Assistant Codex Voice Bridge",
                            "version": "0.6.1",
                        },
                        "capabilities": {"experimentalApi": True},
                    },
                )
                await self.notify("initialized", {})
                self._initialized = True
                await self.assert_no_configured_mcp_servers()
                await self.refresh_account()
            except BaseException:
                await self._stop_process()
                raise

    async def close(self) -> None:
        async with self._lifecycle_lock:
            if self._closed:
                return
            self._closed = True
            self._initialized = False
            await self._stop_process()
            error = AppServerExited("app-server client closed")
            self._fail_pending(error)
            for timer in self._server_request_timers.values():
                timer.cancel()
            self._server_request_timers.clear()
            self._server_response_states.clear()
            self._retired_server_response_ids.clear()
            for subscription in tuple(self._subscriptions):
                subscription.close()

    async def refresh_account(self) -> JsonObject:
        response = await self.call("account/read", {"refreshToken": False})
        account = response.get("account") if isinstance(response, dict) else None
        if isinstance(account, dict):
            auth_mode = account.get("type")
            plan_type = account.get("planType")
            self._auth_mode = auth_mode if isinstance(auth_mode, str) else None
            self._plan_type = plan_type if isinstance(plan_type, str) else None
        else:
            self._auth_mode = None
            self._plan_type = None
        return self.health()

    async def assert_no_configured_mcp_servers(self) -> None:
        """Fail closed if a local or managed config layer injects MCP."""
        response = await self.call(
            "config/read",
            {"cwd": self.cwd, "includeLayers": True},
        )
        layers = response.get("layers") if isinstance(response, dict) else None
        if not isinstance(layers, list):
            raise ProtocolError("config/read returned an invalid layer response")
        for layer in layers:
            config = layer.get("config") if isinstance(layer, dict) else None
            if _contains_configured_mcp(config):
                raise ProtocolError(
                    "Codex configuration contains an MCP server; voice bridge startup denied"
                )

    async def call(
        self,
        method: str,
        params: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> Any:
        process = self.process
        if process is None or process.returncode is not None:
            raise AppServerExited("codex app-server is not running")
        request_id = next(self._ids)
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        try:
            await self._write(
                {"method": method, "id": request_id, "params": dict(params or {})}
            )
            response = await asyncio.wait_for(
                future,
                timeout=self.request_timeout if timeout is None else timeout,
            )
        finally:
            self._pending.pop(request_id, None)
        if isinstance(response, _RpcFailure):
            raise RpcError(method, response.error)
        return response

    async def notify(
        self, method: str, params: Mapping[str, Any] | None = None
    ) -> None:
        await self._write({"method": method, "params": dict(params or {})})

    async def respond_result(
        self, request_id: int | str, result: Mapping[str, Any]
    ) -> None:
        await self._respond(request_id, {"id": request_id, "result": dict(result)})

    async def respond_error(
        self,
        request_id: int | str,
        code: int,
        message: str,
        data: Any | None = None,
    ) -> None:
        error: JsonObject = {"code": code, "message": message}
        if data is not None:
            error["data"] = data
        await self._respond(request_id, {"id": request_id, "error": error})

    def subscribe(self, *, maxsize: int = 512) -> RpcSubscription:
        subscription = RpcSubscription(self, maxsize=maxsize)
        self._subscriptions.add(subscription)
        return subscription

    def unsubscribe(self, subscription: RpcSubscription) -> None:
        self._subscriptions.discard(subscription)

    async def _write(self, message: JsonObject) -> None:
        process = self.process
        if process is None or process.returncode is not None or process.stdin is None:
            raise AppServerExited("codex app-server stdin is unavailable")
        payload = (
            json.dumps(message, separators=(",", ":"), ensure_ascii=False).encode()
            + b"\n"
        )
        async with self._write_lock:
            process.stdin.write(payload)
            try:
                await process.stdin.drain()
            except (BrokenPipeError, ConnectionResetError) as exc:
                raise AppServerExited("codex app-server closed stdin") from exc

    async def _respond(
        self,
        request_id: int | str,
        message: JsonObject,
        *,
        state: _ServerResponseState | None = None,
    ) -> None:
        state = state or self._server_response_states.get(request_id)
        if state is None:
            if request_id in self._retired_server_response_ids:
                return
            await self._write(message)
            return
        async with state.lock:
            if state.written:
                return
            if self._server_response_states.get(request_id) is not state:
                return
            await self._write(message)
            # A completed _write() is the commit boundary. If stdin buffered
            # bytes but drain then raised or was cancelled, delivery is
            # unknowable; leave this state active so one fallback write can
            # still succeed. This never re-executes the Home Assistant call.
            state.written = True
            if self._server_response_states.get(request_id) is state:
                self._server_response_states.pop(request_id)
                self._retire_server_response_id(request_id)
        # The timeout remains responsible until a response write succeeds. If
        # this task stalls, fails, or is cancelled in _write(), the state stays
        # uncommitted and the fallback can take ownership of the next attempt.
        self._cancel_server_request_timeout(request_id, state)

    def _retire_server_response_id(self, request_id: int | str) -> None:
        retired = self._retired_server_response_ids
        retired.pop(request_id, None)
        retired[request_id] = None
        if len(retired) > MAX_RETIRED_SERVER_RESPONSE_IDS:
            retired.pop(next(iter(retired)))

    async def _read_stdout(self) -> None:
        process = self.process
        assert process is not None and process.stdout is not None
        try:
            while line := await process.stdout.readline():
                try:
                    message = json.loads(line)
                except (UnicodeDecodeError, json.JSONDecodeError):  # fmt: skip
                    LOGGER.error("Ignoring malformed app-server stdout line")
                    continue
                if not isinstance(message, dict):
                    LOGGER.error("Ignoring non-object app-server message")
                    continue
                await self._route_message(message)
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("App-server stdout reader failed")
            if process.returncode is None:
                with contextlib.suppress(ProcessLookupError):
                    process.terminate()
        finally:
            self._initialized = False
            returncode = await process.wait()
            error = AppServerExited(f"codex app-server exited with status {returncode}")
            self._fail_pending(error)
            self._broadcast(
                {
                    "method": "bridge/appServerExited",
                    "params": {"returncode": returncode},
                }
            )

    async def _read_stderr(self) -> None:
        process = self.process
        assert process is not None and process.stderr is not None
        while line := await process.stderr.readline():
            text = line.decode(errors="replace").rstrip()
            if text:
                self._last_stderr.append(text)
                LOGGER.debug("codex app-server: %s", text)

    async def _route_message(self, message: JsonObject) -> None:
        request_id = message.get("id")
        method = message.get("method")
        if request_id is not None and isinstance(method, str):
            await self._handle_server_request(message)
            return
        if request_id is not None:
            future = self._pending.get(request_id)
            if future is None or future.done():
                LOGGER.warning(
                    "Received response for unknown app-server request id %r", request_id
                )
                return
            if "error" in message:
                future.set_result(_RpcFailure(message["error"]))
            elif "result" in message:
                future.set_result(message["result"])
            else:
                future.set_exception(
                    ProtocolError("JSON-RPC response has no result or error")
                )
            return
        if isinstance(method, str):
            self._broadcast(message)

    async def _handle_server_request(self, message: JsonObject) -> None:
        method = message["method"]
        request_id = message["id"]
        self._retired_server_response_ids.pop(request_id, None)
        if method in _MODERN_APPROVAL_METHODS:
            await self.respond_result(request_id, {"decision": "decline"})
            return
        if method in _LEGACY_APPROVAL_METHODS:
            await self.respond_result(
                request_id,
                {
                    "decision": {
                        "denied": {
                            "rejection": "Voice bridge denies interactive approvals"
                        }
                    }
                },
            )
            return
        if method in _OTHER_CLOSED_METHODS:
            await self.respond_error(
                request_id, -32000, "Interactive request denied by voice bridge"
            )
            return
        if method != "item/tool/call":
            await self.respond_error(
                request_id, -32601, f"Unsupported server request: {method}"
            )
            return
        state = _ServerResponseState()
        self._server_response_states[request_id] = state
        delivered = self._broadcast(message)
        if not delivered:
            await self.respond_result(
                request_id,
                {
                    "contentItems": [
                        {
                            "type": "inputText",
                            "text": "No voice client is available for this tool call",
                        }
                    ],
                    "success": False,
                },
            )
            return
        timer = asyncio.create_task(
            self._expire_server_request(request_id, state),
            name=f"codex-tool-timeout-{request_id}",
        )
        state.timer = timer
        self._server_request_timers[request_id] = timer

    def _broadcast(self, message: JsonObject) -> int:
        delivered = 0
        for subscription in tuple(self._subscriptions):
            try:
                subscription.queue.put_nowait(message)
                delivered += 1
            except asyncio.QueueFull:
                LOGGER.error("Dropping app-server event for a slow subscriber")
        return delivered

    async def _expire_server_request(
        self, request_id: int | str, state: _ServerResponseState
    ) -> None:
        try:
            await asyncio.sleep(self.tool_timeout)
            await self._respond(
                request_id,
                {
                    "id": request_id,
                    "result": {
                        "contentItems": [
                            {
                                "type": "inputText",
                                "text": (
                                    '{"error":"home_assistant_tool_outcome_unknown",'
                                    '"do_not_retry":true}'
                                ),
                            }
                        ],
                        "success": False,
                    },
                },
                state=state,
            )
        except asyncio.CancelledError:
            pass
        except AppServerExited:
            pass
        finally:
            if self._server_request_timers.get(request_id) is asyncio.current_task():
                self._server_request_timers.pop(request_id)

    def _cancel_server_request_timeout(
        self, request_id: int | str, state: _ServerResponseState
    ) -> None:
        timer = state.timer
        if timer is None or self._server_request_timers.get(request_id) is not timer:
            return
        self._server_request_timers.pop(request_id)
        if timer is not asyncio.current_task():
            timer.cancel()

    def _fail_pending(self, error: BaseException) -> None:
        for future in self._pending.values():
            if not future.done():
                future.set_exception(error)

    async def _stop_process(self) -> None:
        process = self.process
        self._initialized = False
        if process is None:
            return
        if process.stdin is not None:
            process.stdin.close()
            with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                await process.stdin.wait_closed()
        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=5)
            except TimeoutError:
                process.kill()
                await process.wait()
        current = asyncio.current_task()
        for task in (self._reader_task, self._stderr_task):
            if task is not None and task is not current and not task.done():
                task.cancel()
        await asyncio.gather(
            *(
                task
                for task in (self._reader_task, self._stderr_task)
                if task is not None and task is not current
            ),
            return_exceptions=True,
        )
        self.process = None


class _RpcFailure:
    def __init__(self, error: Any) -> None:
        self.error = error


class _ServerResponseState:
    __slots__ = ("lock", "timer", "written")

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.timer: asyncio.Task[None] | None = None
        self.written = False


def _contains_configured_mcp(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    for key, nested in value.items():
        if key in {"mcp_servers", "mcpServers"} and bool(nested):
            return True
        if _contains_configured_mcp(nested):
            return True
    return False
