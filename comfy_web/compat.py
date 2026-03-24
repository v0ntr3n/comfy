from __future__ import annotations

import inspect
import io
import json
import re
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, Callable

from sanic import Sanic
from sanic.exceptions import NotFound as HTTPNotFound
from sanic.exceptions import Unauthorized as HTTPUnauthorized
from sanic.response import HTTPResponse
from sanic.response import file as sanic_file
from sanic.response import json as sanic_json


def _translate_path(path: str) -> str:
    def repl(match: re.Match[str]) -> str:
        name = match.group("name")
        pattern = match.group("pattern")
        if pattern == ".*":
            return f"<{name}:path>"
        return f"<{name}>"

    return re.sub(r"\{(?P<name>[^}:]+)(?::(?P<pattern>[^}]+))?\}", repl, path)


class QueryProxy(Mapping[str, Any]):
    def __init__(self, params):
        self._params = params

    def get(self, key, default=None):
        return self._params.get(key, default)

    def getall(self, key):
        getter = getattr(self._params, "getlist", None)
        if getter is not None:
            return list(getter(key))
        value = self._params.get(key)
        return [] if value is None else [value]

    def __getitem__(self, key):
        value = self._params.get(key)
        if value is None:
            raise KeyError(key)
        return value

    def __iter__(self) -> Iterator[str]:
        return iter(self._params.keys())

    def __len__(self) -> int:
        return len(self._params.keys())


class _RelUrl:
    def __init__(self, query: QueryProxy):
        self.query = query


class UploadedFile:
    def __init__(self, sanic_file):
        self.filename = sanic_file.name
        self.file = io.BytesIO(sanic_file.body)
        self.content_type = getattr(sanic_file, "type", None)


class Request:
    def __init__(self, sanic_request, match_info: dict[str, Any] | None = None):
        self._request = sanic_request
        self.match_info = dict(match_info or {})
        self.headers = sanic_request.headers
        self.method = sanic_request.method
        self.path = sanic_request.path
        self.content_type = sanic_request.content_type
        self.query = QueryProxy(sanic_request.args)
        self.rel_url = _RelUrl(self.query)
        self._websocket = None

    async def json(self):
        return self._request.json

    async def post(self):
        data = {}
        form = self._request.form
        if form:
            for key in form.keys():
                values = form.getlist(key)
                data[key] = values[-1] if values else None

        files = self._request.files
        if files:
            for key in files.keys():
                values = files.getlist(key)
                data[key] = UploadedFile(values[-1]) if values else None

        return data


class _WSMessage:
    def __init__(self, msg_type, data):
        self.type = msg_type
        self.data = data


class _PreparedWebSocket:
    def __init__(self, sanic_ws):
        self._ws = sanic_ws
        self._exception = None
        self._closed = False

    async def send_bytes(self, data: bytes):
        await self._ws.send(data)

    async def send_json(self, data: Any):
        await self._ws.send(json.dumps(data))

    def exception(self):
        return self._exception

    def __aiter__(self):
        return self

    async def __anext__(self):
        import aiohttp

        if self._closed:
            raise StopAsyncIteration

        try:
            data = await self._ws.recv()
        except Exception as exc:
            self._exception = exc
            self._closed = True
            return _WSMessage(aiohttp.WSMsgType.ERROR, None)

        if data is None:
            self._closed = True
            raise StopAsyncIteration
        if isinstance(data, bytes):
            return _WSMessage(aiohttp.WSMsgType.BINARY, data)
        return _WSMessage(aiohttp.WSMsgType.TEXT, data)


class WebSocketResponse:
    def __init__(self):
        self._prepared: _PreparedWebSocket | None = None

    async def prepare(self, request: Request):
        if request._websocket is None:
            raise RuntimeError("WebSocket not attached to request")
        self._prepared = request._websocket
        return self

    async def send_bytes(self, data: bytes):
        await self._prepared.send_bytes(data)

    async def send_json(self, data: Any):
        await self._prepared.send_json(data)

    def exception(self):
        return self._prepared.exception()

    def __aiter__(self):
        return self._prepared.__aiter__()

    async def __anext__(self):
        return await self._prepared.__anext__()


class Response(HTTPResponse):
    def __init__(self, *, status: int = 200, text: str | None = None, body: bytes | str | None = None,
                 content_type: str | None = None, headers: dict[str, str] | None = None):
        if text is not None and body is None:
            body = text
            content_type = content_type or "text/plain; charset=utf-8"
        super().__init__(body=body, status=status, headers=headers, content_type=content_type)

    def enable_compression(self):
        return None


def json_response(body: Any, status: int = 200, headers: dict[str, str] | None = None):
    return sanic_json(body, status=status, headers=headers)


class FileResponse:
    def __init__(self, path: str, headers: dict[str, str] | None = None):
        self.path = path
        self.headers = dict(headers or {})

    def __await__(self):
        mime_type = self.headers.get("Content-Type")
        return sanic_file(self.path, headers=self.headers, mime_type=mime_type).__await__()


@dataclass
class RouteDef:
    method: str
    path: str
    handler: Callable[..., Any]
    kwargs: dict[str, Any] = field(default_factory=dict)


@dataclass
class StaticDef:
    path: str
    directory: str


class RouteTableDef(list):
    def route(self, method: str, path: str, **kwargs):
        def decorator(handler=None, **override_kwargs):
            if handler is None:
                def inner(actual_handler):
                    self.append(
                        RouteDef(
                            method=method.upper(),
                            path=path,
                            handler=actual_handler,
                            kwargs={**kwargs, **override_kwargs},
                        )
                    )
                    return actual_handler

                return inner

            self.append(
                RouteDef(
                    method=method.upper(),
                    path=path,
                    handler=handler,
                    kwargs={**kwargs, **override_kwargs},
                )
            )
            return handler

        return decorator

    def get(self, path: str, **kwargs):
        return self.route("GET", path, **kwargs)

    def post(self, path: str, **kwargs):
        return self.route("POST", path, **kwargs)

    def put(self, path: str, **kwargs):
        return self.route("PUT", path, **kwargs)

    def patch(self, path: str, **kwargs):
        return self.route("PATCH", path, **kwargs)

    def delete(self, path: str, **kwargs):
        return self.route("DELETE", path, **kwargs)

    def head(self, path: str, **kwargs):
        return self.route("HEAD", path, **kwargs)


def middleware(handler):
    return handler


def static(path: str, directory: str):
    return StaticDef(path=path, directory=directory)


class _RouterProxy:
    def __init__(self, app: "Application"):
        self._app = app

    def add_get(self, path: str, handler: Callable[..., Any]):
        routes = RouteTableDef()
        routes.get(path)(handler)
        self._app.add_routes(routes)


class Application:
    def __init__(self, client_max_size: int | None = None, middlewares: list[Callable] | None = None):
        self._app = Sanic(f"ComfyUI-{id(self)}")
        self.middlewares = list(middlewares or [])
        self._registered_routes: list[Any] = []
        if client_max_size is not None:
            self._app.config.REQUEST_MAX_SIZE = client_max_size
        self.router = _RouterProxy(self)

    async def _call_handler(self, handler: Callable[..., Any], request: Request):
        result = handler(request)
        if inspect.isawaitable(result):
            result = await result
        return result

    async def _dispatch(self, handler: Callable[..., Any], request: Request):
        async def run(index: int, current_request: Request):
            if index >= len(self.middlewares):
                return await self._call_handler(handler, current_request)

            middleware_handler = self.middlewares[index]

            async def next_handler(next_request: Request):
                return await run(index + 1, next_request)

            result = middleware_handler(current_request, next_handler)
            if inspect.isawaitable(result):
                result = await result
            return result

        return await run(0, request)

    def _register_route(self, route: RouteDef, prefix: str = ""):
        uri = _translate_path(prefix + route.path)
        is_websocket = route.kwargs.get("websocket", False)

        if is_websocket:
            async def websocket_handler(sanic_request, ws, **params):
                request = Request(sanic_request, params)
                request._websocket = _PreparedWebSocket(ws)
                return await self._call_handler(route.handler, request)

            self._app.add_websocket_route(websocket_handler, uri)
            return

        async def http_handler(sanic_request, **params):
            request = Request(sanic_request, params)
            return await self._dispatch(route.handler, request)

        self._app.add_route(http_handler, uri, methods=[route.method])

    def add_routes(self, routes: Iterable[Any]):
        route_list = list(routes)
        self._registered_routes.extend(route_list)
        for route in route_list:
            if isinstance(route, StaticDef):
                self._app.static(route.path, route.directory)
            else:
                self._register_route(route)

    def add_subapp(self, prefix: str, subapp: "Application"):
        for route in subapp._routes:
            if isinstance(route, StaticDef):
                self._app.static(prefix + route.path, route.directory)
            else:
                self._register_route(route, prefix=prefix)

    @property
    def sanic_app(self):
        return self._app

    @property
    def _routes(self):
        return self._registered_routes

    @property
    def config(self):
        return self._app.config

web = SimpleNamespace(
    Application=Application,
    FileResponse=FileResponse,
    HTTPNotFound=HTTPNotFound,
    HTTPUnauthorized=HTTPUnauthorized,
    Request=Request,
    Response=Response,
    RouteDef=RouteDef,
    RouteTableDef=RouteTableDef,
    StreamResponse=HTTPResponse,
    WebSocketResponse=WebSocketResponse,
    json_response=json_response,
    middleware=middleware,
    static=static,
)
