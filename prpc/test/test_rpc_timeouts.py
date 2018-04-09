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

import pytest

import prpc


@pytest.mark.async_test
async def test_call_timeout(event_loop, rpc_peer):
    """Checks that proper exception is raised on call timeout."""
    CALL_DURATION = 10
    TIMEOUT = 0.5
    async with rpc_peer('util', event_loop) as peer:
        call = peer.connection.call_unary(
            'sleep', [CALL_DURATION], timeout=TIMEOUT
        )
        async with call:
            with pytest.raises(prpc.RpcCallTimeoutError):
                await call.result


@pytest.mark.async_test
async def test_istream_timeout(event_loop, rpc_peer, monkeypatch):
    """Checks that proper exception is raised when caller
    does not consume remote stream data in time.
    """
    READ_TIMEOUT = 0
    monkeypatch.setattr(
        prpc.protocol.constants,
        'STREAM_READ_TIMEOUT',
        READ_TIMEOUT
    )
    async with rpc_peer('streams', event_loop) as peer:
        async with peer.connection.call_istream('endless_stream') as call:
            await asyncio.sleep(READ_TIMEOUT + 1)
            with pytest.raises(prpc.RpcStreamTimeoutError):
                await call.result


@pytest.mark.async_test
async def test_ostream_timeout(event_loop, rpc_local, monkeypatch):
    """Checks that proper exception is raised when callee
    does not consume stream data in time.
    """
    SLEEP_DURATION = 10
    SEND_DELAY = 0.01
    READ_TIMEOUT = 0
    monkeypatch.setattr(
        prpc.protocol.constants,
        'STREAM_READ_TIMEOUT',
        READ_TIMEOUT
    )

    @prpc.method
    async def sleeper(ctx):
        await asyncio.sleep(SLEEP_DURATION, loop=event_loop)

    async with rpc_local({'sleep': sleeper}, None, event_loop) as rpc:
        async with rpc.client_connection.call_ostream('sleep') as call:
            # Exact number of messages to send is important,
            # as stream MAY get closed at any time after
            # 'BUFFER_SIZE + 1' messages.
            buffer_size = prpc.protocol.constants.STREAM_BUFFER_QUEUE_SIZE
            for _ in range(buffer_size + 1):
                await call.stream.send(b'whatever')
            with pytest.raises(prpc.RpcServerError):
                await call.result
        async with rpc.client_connection.call_ostream('sleep') as call:
            while call.stream.is_open:
                await call.stream.send(b'whatever')
                # Sleep needed so client catches up on server's failure.
                await asyncio.sleep(SEND_DELAY, loop=event_loop)
            with pytest.raises(prpc.RpcServerError):
                await call.result


@pytest.mark.async_test
async def test_connect_timeout(event_loop, rpc_peer, monkeypatch):
    """Checks that proper exception is raised when server doesn't respond with
    handshake in time.
    """
    HANDSHAKE_TIMEOUT = 0
    monkeypatch.setattr(
        prpc.protocol.constants,
        'HANDSHAKE_TIMEOUT',
        HANDSHAKE_TIMEOUT
    )
    with pytest.raises(prpc.RpcConnectionTimeoutError):
        await rpc_peer('echo', event_loop).connect()


@pytest.mark.async_test
async def test_connect_timeout_local(event_loop, rpc_local):
    """Checks that proper exception is raised when server doesn't respond with
    handshake in time with in-process RPC.
    """
    rpc = rpc_local(
        None, None, event_loop,
        connect_kwargs=dict(timeout=0)
    )
    with pytest.raises(prpc.RpcConnectionTimeoutError):
        await rpc.connect()


@pytest.mark.async_test
async def test_accept_timeout_local(event_loop, rpc_local):
    """Checks that proper exception is raised when client doesn't send the
    handshake in time.
    """
    rpc = rpc_local(
        None, None, event_loop,
        accept_kwargs=dict(timeout=0)
    )
    with pytest.raises(prpc.RpcConnectionClosedError):
        await rpc.connect()
