#
# coding: utf-8
# Copyright (c) 2017 DATADVANCE
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

import asyncio
import logging
import pathlib
import re
import sys

import aiohttp
import aiohttp.web

import prpc


class RpcServerProcess(object):
    log = logging.getLogger("RpcServerProcess")

    def __init__(self, service_name, loop):
        self._loop = loop
        self._service = "service_%s" % (service_name,)
        self._process = None
        self._output_monitor = None
        self._port = None

    @property
    def endpoint(self):
        if self._port is not None:
            return "ws://localhost:%d/" % (self._port,)
        return None

    @classmethod
    async def start_process(cls, service, loop):
        PORT_TIMEOUT = 5

        server = pathlib.Path(__file__).absolute(
        ).parent.joinpath("peer/peer.py")

        process = await asyncio.create_subprocess_exec(
            sys.executable, str(server), service,
            stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE, loop=loop
        )

        async def get_port():
            pattern = re.compile("^Port: ([0-9]+)$")
            while True:
                line = await process.stdout.readline()
                if not line:
                    break
                decoded = line.decode(sys.stdout.encoding, errors="ignore")
                sys.stdout.write(decoded)
                match = pattern.match(decoded.strip())
                if match:
                    return int(match.groups(0)[0])
            raise RuntimeError("unable to get RPC server port")

        try:
            port = await asyncio.wait_for(get_port(), PORT_TIMEOUT, loop=loop)
        except Exception as ex:
            process.kill()
            await process.wait()
            raise

        async def forward_stdout():
            while True:
                line = await process.stdout.readline()
                if not line:
                    break
                decoded = line.decode(sys.stdout.encoding, errors="ignore")
                sys.stdout.write(decoded)

        async def forward_stderr():
            while True:
                line = await process.stderr.readline()
                if not line:
                    break
                decoded = line.decode(sys.stdout.encoding, errors="ignore")
                sys.stderr.write(decoded)

        output_monitor = asyncio.gather(
            loop.create_task(forward_stdout()),
            loop.create_task(forward_stderr()),
            loop=loop
        )
        return process, output_monitor, port

    async def start(self):
        self.log.info("Starting RPC peer process, service: %s", self._service)
        process, output_monitor, port = await self.start_process(
            self._service, loop=self._loop
        )
        self._process = process
        self._output_monitor = output_monitor
        self._port = port
        self.log.info("RPC peer process started, pid: %d", self._process.pid)

    async def close(self):
        TERMINATE_TIMEOUT = 3
        self.log.info("Closing RPC peer process")
        try:
            self._process.terminate()
            try:
                await asyncio.wait_for(
                    self._process.wait(),
                    TERMINATE_TIMEOUT,
                    loop=self._loop
                )
            except asyncio.TimeoutError:
                self.log.debug("Terminate timed out, attempting to kill")
                self._process.kill()
                await self._process.wait()
        except ProcessLookupError:
            self.log.debug("Peer process already terminated")
        self.log.info("RPC peer process closed, awaiting output")
        await self._output_monitor
        self.log.info("RPC peer process finalized")


class RpcPeer(object):
    """RPC connection to subprocess.

    Pros:
      * Close to 'true' usecase.

    Cons:
      * Not very convenient to do strange things on the server side,
        everything should be routed through cumbersome commandline API.
    """

    def __init__(self, service_name, loop, local_methods=None):
        self._loop = loop
        self._process = RpcServerProcess(service_name, self._loop)
        self._connection = prpc.Connection(
            local_methods, loop=self._loop,
            logger=logging.getLogger("ClientConnection")
        )

    @property
    def connection(self):
        "Get client connection instance."
        return self._connection

    async def connect(self):
        "Start the server and connect to it."
        await self._process.start()
        # Connect may fail too ;-(.
        try:
            await prpc.platform.ws_aiohttp.connect(
                self._connection,
                self._process.endpoint
            )
        except:
            await self._process.close()
            raise

    async def close(self):
        "Drop the connection and the server process."
        try:
            await self._connection.close()
        finally:
            await self._process.close()

    async def __aenter__(self):
        "Async context manager interface."
        await self.connect()
        return self

    async def __aexit__(self, *ex_args):
        "Async context manager interface."
        await self.close()
        return False


class RpcLocal(object):
    """In-process RPC connection.

    Pros:
      * Allows to easily implement complicated tests.

    Cons:
      * Client and server share the same event loop, not asynchronous enough.
    """

    def __init__(self, server_methods, client_methods, loop,
                 server_kwargs={}, client_kwargs={},
                 accept_kwargs={}, connect_kwargs={}):
        self.endpoint = None
        self._loop = loop
        self._log = logging.getLogger("RpcLocalServer")

        self._server_connection = prpc.Connection(
            server_methods, loop=self._loop,
            logger=logging.getLogger("ServerConnection"),
            debug=True,
            **server_kwargs
        )
        self._client_connection = prpc.Connection(
            client_methods, loop=self._loop,
            logger=logging.getLogger("ClientConnection"),
            debug=True,
            **client_kwargs
        )
        self._accept_kwargs = accept_kwargs
        self._connect_kwargs = connect_kwargs

        self._app = aiohttp.web.Application()
        self._app.router.add_get("/", self._handler)
        self._server = prpc.platform.ws_aiohttp.AsyncServer(
            self._app,
            endpoints=[("127.0.0.1", 0)],
            shutdown_timeout=5.,
            logger=self._log,
            loop=self._loop
        )

    @property
    def client_connection(self):
        "Get client-side connection."
        return self._client_connection

    @property
    def server_connection(self):
        "Get server-side connection."
        return self._server_connection

    async def connect(self):
        "Start the async server and connect to it."
        (host, port), = await self._server.start()
        self.endpoint = "ws://%s:%d" % (host, port)
        await prpc.platform.ws_aiohttp.connect(
            self._client_connection,
            self.endpoint,
            **self._connect_kwargs
        )
        self._log.info("Connection established")

    async def close(self):
        "Close the client connection and shutdown the server."
        self.endpoint = None
        await self._client_connection.close()
        self._log.info("Connection closed")
        await self._server.shutdown()

    async def _handler(self, request):
        "Aiohttp incoming request handler."
        return await prpc.platform.ws_aiohttp.accept(
            self._server_connection, request, **self._accept_kwargs
        )

    async def __aenter__(self):
        "Async context manager interface."
        await self.connect()
        return self

    async def __aexit__(self, *exc_args):
        "Async context manager interface."
        await self.close()
        return False
