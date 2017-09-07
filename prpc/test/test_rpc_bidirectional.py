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

import pytest

import prpc


@pytest.mark.async_test
async def test_echo_callback(event_loop, rpc_peer):
    """Checks that proper exception is raised when caller
    does not consume remote stream in time.
    """
    ARGS = [123, b"123"]
    KWARGS = {"echo": "callback"}

    class CallbackService(object):
        def __init__(self):
            self.result = None
            self._reset()

        async def check(self):
            args, kwargs = await self.result
            assert args == tuple(ARGS)
            assert kwargs == KWARGS
            self._reset()

        @prpc.method
        async def callback(self, ctx, *args, **kwargs):
            self.result.set_result((args, kwargs))

        def _reset(self):
            self.result = event_loop.create_future()

    service = CallbackService()

    async with rpc_peer("echo", event_loop, local_methods=service) as peer:
        call = peer.connection.call_unary(
            "echo_callback", ["callback"] + ARGS, KWARGS
        )
        async with call:
            await call.result
        await service.check()

        call = peer.connection.call_unary(
            "echo_callback", ["callback"] + ARGS, KWARGS
        )
        async with call:
            # Absence of 'await call.result' leads to interesting behavior:
            #   - outgoing echo_callback gets cancelled (by call.__aexit__)
            #   - callback arrives fine
            #   - callback result arrives back to peer, but there is no
            #     outgoing call to receive this result
            #
            # From the autotest perspective we only check that there are
            # no weird deadlocks.
            #
            # Manual parsing of logs, however, shows an interesing picture )
            await service.check()


@pytest.mark.async_test
async def test_recursive_call(event_loop, rpc_local):
    "Checks advanced usage: deep recursive calls."
    METHOD_NAME = "recursive"
    MAX_DEPTH = 16

    @prpc.method
    async def recursive(ctx, depth=0):
        if depth < MAX_DEPTH:
            return await ctx.connection.call_simple(METHOD_NAME, depth + 1)
        return MAX_DEPTH

    methods = {METHOD_NAME: recursive}
    async with rpc_local(methods, methods, event_loop) as rpc:
        result = await rpc.client_connection.call_simple(METHOD_NAME)
        assert result == MAX_DEPTH
