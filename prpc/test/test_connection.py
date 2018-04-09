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
import collections

import pytest

import prpc


@pytest.mark.async_test
async def test_properties(event_loop, rpc_local):
    "Check connection public properties and (very) basic state transitions."
    rpc = rpc_local(None, None, event_loop)

    assert not rpc.client_connection.connected
    assert not rpc.server_connection.connected
    assert rpc.client_connection.state == prpc.ConnectionState.NEW
    assert rpc.server_connection.state == prpc.ConnectionState.NEW
    assert rpc.client_connection.mode == prpc.ConnectionMode.NEW
    assert rpc.server_connection.mode == prpc.ConnectionMode.NEW
    assert rpc.client_connection.id == None
    assert rpc.server_connection.id == None
    async with rpc:
        assert rpc.client_connection.connected
        assert rpc.server_connection.connected
        assert rpc.client_connection.state == prpc.ConnectionState.CONNECTED
        assert rpc.server_connection.state == prpc.ConnectionState.CONNECTED
        assert rpc.client_connection.mode == prpc.ConnectionMode.CLIENT
        assert rpc.server_connection.mode == prpc.ConnectionMode.SERVER
        assert rpc.client_connection.id == rpc.server_connection.id
        assert rpc.client_connection.id is not None
    assert not rpc.client_connection.connected
    assert not rpc.server_connection.connected
    assert rpc.client_connection.state == prpc.ConnectionState.CLOSED
    assert rpc.server_connection.state == prpc.ConnectionState.CLOSED
    assert rpc.client_connection.mode == prpc.ConnectionMode.CLIENT
    assert rpc.server_connection.mode == prpc.ConnectionMode.SERVER
    assert rpc.client_connection.id == rpc.server_connection.id
    assert rpc.client_connection.id is not None


def test_attached_data():
    "Check connection attached data property."
    INIT_VALUE = {'some': 'data'}
    connection = prpc.Connection(attached_data=INIT_VALUE)
    assert connection.data == INIT_VALUE
    MOD_KEY = 'more'
    MOD_VALUE = 'attached data'
    connection.data[MOD_KEY] = MOD_VALUE
    assert connection.data[MOD_KEY] == MOD_VALUE
    del connection.data[MOD_KEY]
    assert MOD_KEY not in connection.data
    assert connection.data == INIT_VALUE


@pytest.mark.async_test
async def test_handshake_data(event_loop, rpc_local):
    "Checks that custom handshake payload is successfully delivered."
    SERVER_DATA = b'server data'
    CLIENT_DATA = 'client data'

    rpc = rpc_local(
        None, None, event_loop,
        accept_kwargs=dict(handshake_data=SERVER_DATA),
        connect_kwargs=dict(handshake_data=CLIENT_DATA)
    )
    async with rpc:
        assert rpc.client_connection.handshake_data == SERVER_DATA
        assert rpc.server_connection.handshake_data == CLIENT_DATA


@pytest.mark.async_test
async def test_unserializable_handshake_data(event_loop, rpc_local):
    "Checks that unserializable handshake data raises exceptions immediately."
    def UNSERIALIZABLE(): return 'oops'

    rpc = rpc_local(
        None, None, event_loop,
        accept_kwargs=dict(handshake_data=None),
        connect_kwargs=dict(handshake_data=UNSERIALIZABLE)
    )
    with pytest.raises(TypeError):
        await rpc.connect()

    rpc = rpc_local(
        None, None, event_loop,
        accept_kwargs=dict(handshake_data=UNSERIALIZABLE),
        connect_kwargs=dict(handshake_data=None)
    )
    with pytest.raises(prpc.RpcConnectionClosedError):
        await rpc.connect()


@pytest.mark.async_test
async def test_close_ongoing_calls(event_loop, rpc_local):
    "That connection closes promptly even with long-running calls."
    # Unreasonably long sleep.
    SLEEP_DURATION = 30
    # Max close duration (still unreasonably long).
    MAX_DURATION = 10
    # Small wait to ensure that calls are actually in progress.
    CLOSE_DELAY = 0.1

    @prpc.method
    async def sleep(ctx):
        await asyncio.sleep(SLEEP_DURATION, loop=ctx.loop)

    methods = {'sleep': sleep}

    for to_close in ['client_connection', 'server_connection']:
        async with rpc_local(methods, methods, event_loop) as rpc:
            assert not rpc.client_connection.active
            assert not rpc.server_connection.active
            start = event_loop.time()
            # Start call 'client -> server'
            c1 = await rpc.client_connection.call_unary('sleep')
            assert rpc.client_connection.active
            # Start call 'server -> client'
            c2 = await rpc.server_connection.call_unary('sleep')
            assert rpc.server_connection.active
            await asyncio.sleep(CLOSE_DELAY, loop=event_loop)
            await getattr(rpc, to_close).close()
            with pytest.raises(prpc.RpcConnectionClosedError):
                await c1.result
            with pytest.raises(prpc.RpcConnectionClosedError):
                await c2.result
            assert (event_loop.time() - start) < MAX_DURATION


@pytest.mark.async_test
async def test_server_connect_callback(event_loop, rpc_local):
    "That accept callback works as expected on server side."

    CLIENT_DATA = {'token': '0xdeadbeef'}

    ACCEPT_RESULTS = [False, False]

    def sync_accept(connection, handshake):
        assert handshake == CLIENT_DATA
        ACCEPT_RESULTS[0] = True

    async def async_accept(connection, handshake):
        assert handshake == CLIENT_DATA
        ACCEPT_RESULTS[1] = True

    rpc = rpc_local(
        None, None, event_loop,
        accept_kwargs=dict(connect_callback=sync_accept),
        connect_kwargs=dict(handshake_data=CLIENT_DATA)
    )
    async with rpc:
        assert rpc.client_connection.handshake_data == None
        assert rpc.server_connection.handshake_data == CLIENT_DATA

    rpc = rpc_local(
        None, None, event_loop,
        accept_kwargs=dict(connect_callback=async_accept),
        connect_kwargs=dict(handshake_data=CLIENT_DATA)
    )
    async with rpc:
        assert rpc.client_connection.handshake_data == None
        assert rpc.server_connection.handshake_data == CLIENT_DATA

    assert ACCEPT_RESULTS == [True, True]

    rpc = rpc_local(
        None, None, event_loop,
        accept_kwargs=dict(connect_callback=async_accept),
        connect_kwargs=dict(handshake_data="wrong handshake")
    )
    with pytest.raises(prpc.RpcConnectionClosedError):
        await rpc.connect()


@pytest.mark.async_test
async def test_client_connect_callback(event_loop, rpc_local):
    "That connect callback works as expected on client side."

    SERVER_DATA = {'token': '0xdeadbeef'}

    ACCEPT_RESULTS = [False, False]

    def sync_accept(connection, handshake):
        assert handshake == SERVER_DATA
        ACCEPT_RESULTS[0] = True

    async def async_accept(connection, handshake):
        assert handshake == SERVER_DATA
        ACCEPT_RESULTS[1] = True

    rpc = rpc_local(
        None, None, event_loop,
        accept_kwargs=dict(handshake_data=SERVER_DATA),
        connect_kwargs=dict(connect_callback=sync_accept)
    )
    async with rpc:
        assert rpc.server_connection.handshake_data is None
        assert rpc.client_connection.handshake_data == SERVER_DATA

    rpc = rpc_local(
        None, None, event_loop,
        accept_kwargs=dict(handshake_data=SERVER_DATA),
        connect_kwargs=dict(connect_callback=async_accept)
    )
    async with rpc:
        assert rpc.server_connection.handshake_data is None
        assert rpc.client_connection.handshake_data == SERVER_DATA

    assert ACCEPT_RESULTS == [True, True]

    rpc = rpc_local(
        None, None, event_loop,
        accept_kwargs=dict(handshake_data="wrong handshake"),
        connect_kwargs=dict(connect_callback=async_accept)
    )
    with pytest.raises(prpc.RpcConnectionRejectedError):
        await rpc.connect()


@pytest.mark.async_test
async def test_serve_traceback(event_loop, rpc_local):
    """Check that connection serve_traceback option
    actually disables the remote traceback.
    """
    @prpc.method
    async def raiser(ctx):
        raise ValueError('oops')

    rpc = rpc_local({'raiser': raiser}, None, event_loop)
    async with rpc:
        try:
            await rpc.client_connection.call_simple('raiser')
        except prpc.RpcMethodError as ex:
            assert ex.cause_type == 'ValueError'
            assert 'ValueError' in ex.remote_traceback

    rpc = rpc_local(
        {'raiser': raiser}, None, event_loop,
        server_kwargs=dict(serve_traceback=False)
    )
    async with rpc:
        try:
            await rpc.client_connection.call_simple('raiser')
        except prpc.RpcMethodError as ex:
            assert ex.cause_type == 'ValueError'
            assert 'no traceback' in ex.remote_traceback


@pytest.mark.async_test
async def test_method_not_found(event_loop, rpc_local):
    """Check that expected exceptions are raised on unavailable method names.
    """
    @prpc.method
    async def whatever(ctx):
        """does absolutely nothing"""

    rpc = rpc_local({'whatever': whatever}, None, event_loop)
    async with rpc:
        # Check plain wrong name.
        with pytest.raises(prpc.RpcMethodNotFoundError):
            await rpc.client_connection.call_simple('no_such_method')
        with pytest.raises(prpc.RpcMethodNotFoundError):
            await rpc.server_connection.call_simple('no_methods_at_all')

    class BrokenLocator(prpc.AbstractMethodLocator):
        def resolve(self, method_name, call_type, connection):
            return 'not a method'

    rpc = rpc_local(BrokenLocator(), None, event_loop)
    async with rpc:
        # Check non-callable method.
        with pytest.raises(prpc.RpcServerError):
            await rpc.client_connection.call_simple("whatever")


@pytest.mark.async_test
async def test_custom_locator(event_loop, rpc_local):
    """Check that passed locator is actually used.
    """
    CALL_COUNT = 10

    @prpc.method
    async def the_method(ctx):
        """does absolutely nothing"""

    class CallStatsLocator(prpc.TreeMethodLocator):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.call_stats = collections.defaultdict(int)

        def _decorate_method(self, method):
            async def wrapped(ctx, *args, **kwargs):
                assert ctx.locator == self
                ctx.locator.call_stats[ctx.method] += 1
                return await method(ctx, *args, **kwargs)
            return wrapped

    locator = CallStatsLocator({'the_method': the_method})
    rpc = rpc_local(locator, None, event_loop)
    async with rpc:
        assert rpc.server_connection.locator is locator
        assert locator.call_stats['the_method'] == 0
        for idx in range(CALL_COUNT):
            await rpc.client_connection.call_simple('the_method')
            assert locator.call_stats['the_method'] == idx + 1
