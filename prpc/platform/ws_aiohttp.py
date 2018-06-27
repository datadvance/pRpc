#
# coding: utf-8
# Copyright (c) 2018 DATADVANCE
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import asyncio
import collections
import logging
import socket

import aiohttp
import aiohttp.web

from . import generic
from .. import utils


class Websocket(generic.MessageSocket):
    def __init__(self, websocket):
        """Initialize new Websocket instance.

        Args:
            websocket: aiohttp websocket to wrap
                (WebSocketResponse or ClientWebSocketResponse).
        """
        self._ws = websocket

    @property
    def closed(self):
        """Get socket closed flag."""
        return self._ws.closed

    @property
    def close_code(self):
        """Get socket close code."""
        return self._ws.close_code

    async def receive(self):
        """Receive single message.

        Returns:
            A tuple (message_type, payload).
        """
        msg = await self._ws.receive()
        if msg.type == aiohttp.WSMsgType.BINARY:
            return generic.MessageType.BINARY, msg.data
        elif msg.type == aiohttp.WSMsgType.TEXT:
            return generic.MessageType.TEXT, msg.data
        elif msg.type in (aiohttp.WSMsgType.CLOSE,
                          aiohttp.WSMsgType.CLOSING,
                          aiohttp.WSMsgType.CLOSED):
            return generic.MessageType.CLOSE, None
        else:
            return generic.MessageType.ERROR, None

    async def send(self, data):
        """Send data message.

        Args:
            data: Message payload (str or bytes).
        """
        return await self._ws.send_bytes(data)

    def close(self, code=generic.CLOSE_CODE_SUCCESS, message=b''):
        """Close the socket.

        Args:
            code: Close code to send to peer.
            message: Close reason to send to peer.
        """
        return self._ws.close(code=code, message=message)

    def __aiter__(self):
        """Async iterator interface."""
        return self

    async def __anext__(self):
        """Async iterator interface."""
        msg_type, data = await self.receive()
        if msg_type == generic.MessageType.CLOSE:
            raise StopAsyncIteration()
        return msg_type, data


class AsyncServer(object):
    """Starts aiohttp application without blocking the event loop.

    Essentially, it is an utility class to get around aiohttp API deficiencies.
    Mostly inspired by aiohttp.web.run_app, but with some improvements
    and simplification (e.g. no support for unix domain sockets, oh my).

    Can be used as an async context manager (or as a part of one).

    Args:
        app: aiohttp.web.Application instance.
        endpoints: List containing (bound) sockets or pairs (host, port).
        ssl_context: SSL context (from *ssl* stdlib module).
        backlog: Backlog value to use on socket.listen.
        logger: Logger object to use.
        access_log_format: Access log format; has some custom format specifiers,
            please refer to aiohttp docs.
        shutdown_timeout: Server shutdown timeout.
        loop: asyncio event loop instance (asyncio.get_event_loop() by default).

    Note:
        Requires some care to implement correct handling of KeyboardInterrupt,
        as it can essentially happen anywhere.
    """
    # Default logger name.
    DEFAULT_LOG_NAME = 'AsyncServer'
    # Taken from aiohttp.
    DEFAULT_SHUTDOWN_TIMEOUT = 60.
    # Taken from aiohttp.
    DEFAULT_BACKLOG = 128

    def __init__(self, app, endpoints, ssl_context=None,
                 backlog=None, logger=None, access_log_format=None,
                 shutdown_timeout=None, loop=None):
        # TODO: think about TCP_NODELAY, TCP_QUICKACK options to reduce
        # latency if needed.
        self._loop = loop or asyncio.get_event_loop()
        self._app = app
        self._endpoints = endpoints
        if not isinstance(self._endpoints, collections.Iterable):
            self._endpoints = [self._endpoints]
        self._ssl_context = ssl_context
        self._backlog = backlog
        if self._backlog is None:
            self._backlog = self.DEFAULT_BACKLOG
        self._log = logger or logging.getLogger(self.DEFAULT_LOG_NAME)
        self._access_log_format = access_log_format
        self._shutdown_timeout = (
            self.DEFAULT_SHUTDOWN_TIMEOUT
            if shutdown_timeout is None
            else shutdown_timeout
        )

        self._shutdown_event = asyncio.Event(loop=self._loop)
        self._running = False
        self._handler = None
        self._servers = None

    async def start(self):
        """Start the server using the configuration from c-tor."""
        if self._running:
            raise RuntimeError('server is already running')
        self._shutdown_event.clear()
        # Aiohttp internals don't accept None as log format.
        #
        # Reason: nested (**kwargs) calls will break the proper default on
        # aiohttp.web_protocol.RequestHandler.
        make_handler_kwargs = {}
        if self._access_log_format is not None:
            make_handler_kwargs['access_log_format'] = self._access_log_format
        self._handler = self._app.make_handler(
            loop=self._loop, access_log=self._log, **make_handler_kwargs
        )

        # Call application on_startup hooks.
        await self._app.startup()

        server_init_coros = []
        for endpoint in self._endpoints:
            if isinstance(endpoint, socket.socket):
                server_init_coros.append(
                    self._loop.create_server(
                        self._handler, sock=endpoint,
                        ssl=self._ssl_context, backlog=self._backlog
                    )
                )
            else:
                host, port = endpoint
                server_init_coros.append(
                    self._loop.create_server(
                        self._handler, host, port,
                        ssl=self._ssl_context, backlog=self._backlog
                    )
                )

        self._servers = await asyncio.gather(
            *server_init_coros, loop=self._loop
        )
        self._running = True

        listen_endpoints = [
            sock.getsockname() for srv in self._servers for sock in srv.sockets
        ]

        proto = 'https' if self._ssl_context else 'http'
        endpoints_string = ', '.join(
            [
                '%s://%s:%s' % (proto, host, port)
                for (host, port) in listen_endpoints
            ]
        )
        self._log.info('Listening on %s', endpoints_string)
        return listen_endpoints

    async def wait(self):
        """Wait until server shutdown. Return immediately if it not running."""
        if not self._running:
            return
        await self._shutdown_event.wait()

    async def shutdown(self, timeout=None):
        """Shutdown the server.

        Note:
            As taken from aiohttp, 'timeout' is not really enforced enough.
            Harsher shutdown mode may be required. You can always just stop the
            event loop, but peers may get confused a bit.
        """
        if not self._running:
            raise RuntimeError('server is not running')
        if timeout is None:
            timeout = self._shutdown_timeout
        self._log.debug('Shutting down (timeout: %s)', timeout)
        server_close_coros = []
        for server in self._servers:
            server.close()
            server_close_coros.append(server.wait_closed())
        await asyncio.gather(*server_close_coros, loop=self._loop)
        await self._app.shutdown()
        await self._handler.shutdown(timeout)
        await self._app.cleanup()
        self._log.debug('Shutdown complete')
        self._shutdown_event.set()

    async def __aenter__(self):
        """Async context manager interface."""
        endpoints = await self.start()
        return endpoints

    async def __aexit__(self, exc_type, exc_obj, exc_tb):
        """Async context manager interface."""
        await self.shutdown()
        return False


def make_websocket_factory(session, url, **kwargs):
    """Create a new websocket factory.

    Args:
      session: aiohttp.ClientSession instance.
      url: Server url (acceptible schemes: http/https/ws/wss).

    Note:
      kwargs are passed directly to aiohttp
    """
    fixed_kwags = {
        'autoclose': True,
        'autoping': True,
        'protocols': (generic.WS_PROTOCOL_NAME,),
        'heartbeat': generic.WS_HEARTBEAT
    }
    # TODO: investigate connection heartbeat more
    # (Note: hearbeat interval must be greater then stream_read_timeout...).
    kwargs.update(fixed_kwags)

    async def ws_factory():
        raw_ws = await session.ws_connect(url, **kwargs)
        return Websocket(raw_ws)
    return ws_factory


async def accept(connection, request, **kwargs):
    """Accept an incoming connection using aiohttp API.

    Args:
        connection: RPC Connection instance.
        request: aiohttp request.

    Note:
        kwargs are passed directly to connection.accept().
    """
    # TODO: get peer params and store it into connection (e.g. peer ip/port) ?
    ws = aiohttp.web.WebSocketResponse(
        autoping=True,
        protocols=(generic.WS_PROTOCOL_NAME,)
    )
    await ws.prepare(request)
    await connection.accept(Websocket(ws), **kwargs)
    return ws


async def connect(connection, url, **kwargs):
    """Connect to a server using aiohttp API.

    Args:
        connection: RPC Connection instance.
        url: Server url (http/https/ws/wss) or socket factory.

    Note:
        kwargs are passed directly to connection.connect().
    """
    session = aiohttp.ClientSession(loop=connection.loop)
    if callable(url):
        ws_factory = url
    else:
        ws_factory = make_websocket_factory(session, url)

    async def close_session(conn):
        # TODO: It's unclear whether session.close is coroutine or not.
        # It changed across the versions of aiohttp.
        # From github discussions it looks like it was a coroutine,
        # now it is not, but it will be.
        # Changes happen right now, this 'universal kludge' should
        # be removed when it becomes more clear.
        await utils.asyncio.call_sync_async(session.close)
        conn.on_close.remove(close_session)

    try:
        await connection.connect(ws_factory, **kwargs)
        # TODO: avoid multiple close_session callbacks in session.
        # MB callback should 'self remove'.
        # But we probably should make shallow copy of subscriptions in Signal.
        connection.on_close.append(close_session)
    except:
        await utils.asyncio.call_sync_async(session.close)
        raise
    return connection
