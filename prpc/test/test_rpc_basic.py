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
import time

import pytest

import prpc


@pytest.mark.async_test
async def test_echo_unary(event_loop, rpc_peer):
    """Run echo call. Basic sanity check.
    """
    ARGS_LIST = [
        ['it is alive', 1, 1., True, b'!'],
        [['nested lists?'], ['should work']],
        [{'dicts': 'should work too', b'even': b'with binary data'}]
    ]
    async with rpc_peer('echo', event_loop) as peer:
        for args in ARGS_LIST:
            async with peer.connection.call_unary('echo_args', args) as call:
                response = await call.result
            assert response == args
            response = await peer.connection.call_simple('echo_args', *args)
            assert response == args
            kwargs = {'some_key': 'some_value', 'custom': args}
            response = await peer.connection.call_simple(
              'echo_kwargs', **kwargs
            )
            assert response == kwargs


@pytest.mark.async_test
async def test_echo_stream(event_loop, rpc_peer):
    """Run stream echo call. Sanity check for stream calls.
    """
    MESSAGES = [
        'text_message',
        ('long_text_message ' * 1024),
        b'binary_message'
    ]
    RETVAL = 'OK'
    async with rpc_peer('echo', event_loop) as peer:
        async with peer.connection.call_bistream('echo_stream', [RETVAL]) as call:
            for msg in MESSAGES:
                await call.stream.send(msg)
                response = await call.stream.receive()
                assert response == msg
            retval = await call.result
            assert retval == RETVAL


@pytest.mark.async_test
async def test_echo_adaptive(event_loop, rpc_peer):
    "Run all call modes on a single adaptive method."
    DATA = b'I am data'
    async with rpc_peer('echo', event_loop) as peer:
        async with peer.connection.call_unary(
              'echo_adaptive', [DATA]
            ) as call:
            assert [DATA] == await call.result
        async with peer.connection.call_istream(
                'echo_adaptive', [DATA]
            ) as call:
            assert await call.stream.receive() == [DATA]
            assert await call.result == None
        async with peer.connection.call_ostream(
                'echo_adaptive', [DATA]
            ) as call:
            await call.stream.send(DATA)
            assert await call.result == DATA
        async with peer.connection.call_bistream(
                'echo_adaptive', [DATA]
            ) as call:
            await call.stream.send(DATA)
            assert await call.stream.receive() == DATA
            assert await call.result == None


@pytest.mark.async_test
async def test_ostream_counter(event_loop, rpc_peer):
    """Check that output stream is timely closed on both sides.
    """
    MSG_COUNT = 137
    async with rpc_peer('streams', event_loop) as peer:
        # With explicit close.
        async with peer.connection.call_ostream('counter') as call:
            for idx in range(MSG_COUNT):
                await call.stream.send('msg %d' % idx)
            await call.stream.close()
            assert MSG_COUNT == await call.result
        # Without explicit close.
        async with peer.connection.call_ostream('counter') as call:
            for idx in range(MSG_COUNT):
                await call.stream.send('msg %d' % idx)
            assert MSG_COUNT == await call.result
        # Bistream should work too.
        async with peer.connection.call_bistream('counter') as call:
            for idx in range(MSG_COUNT):
                await call.stream.send('msg %d' % idx)
            assert MSG_COUNT == await call.result


@pytest.mark.async_test
async def test_istream_closed_by_peer(event_loop, rpc_peer):
    """Run stream download call. Check that stream closed by peer 'ends' timely.
    """
    async with rpc_peer('streams', event_loop) as peer:
        async with peer.connection.call_istream('pseudodownload') as call:
            async for msg in call.stream:
                pass
            assert call.stream.is_closed
            await call.result


@pytest.mark.async_test
async def test_cancel(event_loop, rpc_peer):
    """Try to cancel both unary and streaming calls.
    """
    CANCEL_DELAY = 0.1
    DELAY = 30.
    async with rpc_peer('util', event_loop) as peer:
        async with peer.connection.call_unary('sleep', [DELAY]) as call:
            await asyncio.sleep(CANCEL_DELAY)
            cancel_sent = await call.cancel()
            assert cancel_sent
            with pytest.raises(prpc.RpcCancelledError):
                await call.result
    async with rpc_peer('echo', event_loop) as peer:
        async with peer.connection.call_bistream('echo_stream', ['']) as call:
            await asyncio.sleep(CANCEL_DELAY)
            cancel_sent = await call.cancel()
            assert cancel_sent
            with pytest.raises(prpc.RpcCancelledError):
                await call.result
        async with peer.connection.call_bistream('echo_stream', ['']) as call:
            await asyncio.sleep(CANCEL_DELAY)


@pytest.mark.async_test
async def test_simple_raise(event_loop, rpc_peer):
    """Check basic error routing - RpcMethodError is raised and contains
    info about the actual remote error.
    """
    REMOTE_EXCEPTIONS = [
        ('RuntimeError', 'some message?'),
        ('ValueError', 'another message?')
    ]
    async with rpc_peer('util', event_loop) as peer:
        for exc_type, exc_msg in REMOTE_EXCEPTIONS:
            call = peer.connection.call_unary(
                'raise_builtin', [exc_type, exc_msg]
            )
            async with call:
                with pytest.raises(prpc.RpcMethodError):
                    try:
                        await call.result
                    except prpc.RpcMethodError as ex:
                        assert ex.cause_type == exc_type
                        assert ex.cause_message == exc_msg
                        assert exc_type in ex.remote_traceback
                        raise


@pytest.mark.async_test
async def test_parallel_call(event_loop, rpc_peer):
    """Test that multiple parallel calls are actually done in parallel.

    Important thing to check is that calls are properly forked into tasks
    and don't block the main listen loop.

    Note:
      Time-based tests are bad in general, but the 10x timeout margin
      should make it reliable.
    """
    # Delay of a single call.
    SLEEP_DELAY = 0.1
    # Number of parallel calls.
    COUNT = 1000
    # Max time to finish all the calls - much smaller then SLEEP_DELAY * COUNT.
    MAX_TIME = 3.
    async with rpc_peer('util', event_loop) as peer:
        start = time.monotonic()
        calls = [
            peer.connection.call_simple('sleep', SLEEP_DELAY)
            for _ in range(COUNT)
        ]
        await asyncio.gather(*calls, loop=event_loop)
        assert (time.monotonic() - start) < MAX_TIME


@pytest.mark.async_test
async def test_unserializable_args(event_loop, rpc_peer):
    """Test exceptions on unserializable call argsuments.
    """
    def UNSERIALIZABLE(): return 'does not work'
    async with rpc_peer('echo', event_loop) as peer:
        # Unserializable args are fatal, call 'never happens'.
        with pytest.raises(TypeError):
            await peer.connection.call_simple('echo', UNSERIALIZABLE)


@pytest.mark.async_test
async def test_unserializable_stream(event_loop, rpc_peer):
    """Test exceptions on unserializable data in stream.
    """
    def UNSERIALIZABLE(): return 'does not work'
    SERIALIZABLE = 'works'
    async with rpc_peer('echo', event_loop) as peer:
        # Unserializable stream data can be handled and safely ignored.
        async with peer.connection.call_bistream('echo_stream') as call:
            with pytest.raises(TypeError):
                await call.stream.send(UNSERIALIZABLE)
            # But call isn't closed.
            await call.stream.send(SERIALIZABLE)
            assert await call.stream.receive() == SERIALIZABLE
            await call.result


@pytest.mark.async_test
async def test_unserializable_return(event_loop, rpc_local):
    "Test exception on unseriazable return value."

    @prpc.method
    async def broken_return(ctx):
        return lambda: 'unserializable'

    rpc = rpc_local(
        {'broken_return': broken_return}, None, event_loop,
    )

    async with rpc:
        with pytest.raises(prpc.RpcMethodError):
            await rpc.client_connection.call_simple('broken_return')
