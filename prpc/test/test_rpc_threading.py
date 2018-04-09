#
# coding: utf-8
# Copyright (c) 2018 DATADVANCE
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
import queue
import threading
import uuid

import aiohttp.web
import pytest

import prpc


class Service(object):
    """Simple RPC service for threaded test.

    Doesn't aim for anything in particular,
    just tries to hit as much implementation code as possible.
    """
    @prpc.method
    async def echo(self, ctx, value):
        return value

    @prpc.method
    async def echo_stream(self, ctx):
        async for msg in ctx.stream:
            await ctx.stream.send(msg)

    @prpc.method
    async def sleep(self, ctx, delay=10):
        await asyncio.sleep(delay, loop=ctx.loop)

    @prpc.method
    async def error(self, ctx):
        raise RuntimeError('planned failure')


def server_thread(queue, service_cls, event_loop_timeout=10.):
    """Server thread function.

    Args:
      queue (queue.Queue): server event queue (passes port back to caller)
      service_cls (type): rpc service type to instanciate
    """
    loop = asyncio.new_event_loop()

    async def accept_handler(request):
        r = await prpc.platform.ws_aiohttp.accept(
            request.app['connection'], request
        )
        return r

    async def run_server():
        app = aiohttp.web.Application()
        app.router.add_get('/', accept_handler)
        server = prpc.platform.ws_aiohttp.AsyncServer(
            app,
            endpoints=[('127.0.0.1', 0)],
            shutdown_timeout=1.,
            logger=logging.getLogger('AsyncServer'),
            loop=loop
        )
        connection = prpc.Connection(
            service_cls(),
            loop=loop,
            logger=logging.getLogger('ServerConnection'),
            debug=True
        )
        # Must send shutdown to a task, as calling it
        # directly from on_close creates a nice circular 'deadlock':
        #
        # * on_close will await the shutdown
        # * shutdown will await for all incoming connections to die
        # * accept_handler (so, connection) waits for connection.accept
        # * connection.accept waits for on close

        def schedule_shutdown(conn):
            loop.create_task(server.shutdown())

        connection.on_close.append(schedule_shutdown)
        app['connection'] = connection
        (_, port), = await server.start()
        queue.put(port)
        # Hard timeout/kill switch (say no to hanging tests!).
        await asyncio.wait([server.wait()], timeout=event_loop_timeout)

    loop.run_until_complete(run_server())


def client_thread(port):
    """Client thread function.

    Args:
      port (int): server's port, must be already bound and listening
    """
    loop = asyncio.new_event_loop()
    url = 'ws://%s:%d' % ('127.0.0.1', port)

    async def run_client():
        ECHO_DATA = [b'data', 1, 2, 3]
        STREAM_ITERATIONS = 100
        CALL_START_DELAY = 0.1

        connection = prpc.Connection(
            None, loop=loop,
            logger=logging.getLogger('ClientConnection'),
            debug=True
        )
        await prpc.platform.ws_aiohttp.connect(connection, url)
        assert connection.connected
        assert ECHO_DATA == await connection.call_simple('echo', ECHO_DATA)
        async with connection.call_bistream('echo_stream') as call:
            for _ in range(STREAM_ITERATIONS):
                data = uuid.uuid4().bytes
                await call.stream.send(data)
                assert await call.stream.receive() == data
                data = uuid.uuid4().hex
                await call.stream.send(data)
                assert await call.stream.receive() == data
            await call.result
        with pytest.raises(prpc.RpcMethodError):
            await connection.call_simple('error')
        async with connection.call_unary('sleep') as call:
            await asyncio.sleep(CALL_START_DELAY, loop=loop)
            await call.cancel()
            with pytest.raises(prpc.RpcCancelledError):
                await call.result

        await connection.close()
        assert not connection.connected

    loop.run_until_complete(run_client())


def test_client_server_different_threads(test_log):
    """Check that RPC connections work fine while running not on
    'main' asyncio evenloop.

    Starts client and server in two separate threads.
    RPC calls that are run in client are not too important,
    most important check there is that nothing deadlocks.

    Discounting framework bugs, it aims checks that the RPC code
    always uses the passes loop parameter and never falls
    into 'asyncio.get_event_loop' default.

    Warning:
      Advanced and kinda convoluted test. Take care.
    """
    server_events = queue.Queue()

    test_log.info('Starting the server thread...')
    server = threading.Thread(
        target=server_thread, args=(server_events, Service)
    )
    server.start()

    test_log.info('Waiting for port...')
    port = server_events.get()
    test_log.info('Port: %d', port)

    test_log.info('Starting the client thread...')
    client = threading.Thread(target=client_thread, args=(port,))
    client.start()

    test_log.info('Waiting for server to shutdown...')
    server.join()
