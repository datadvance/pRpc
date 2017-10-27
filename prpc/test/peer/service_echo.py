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

import logging
import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).absolute().parents[3]))
import prpc


class Service(object):
    log = logging.getLogger('EchoService')

    @prpc.method
    async def echo_args(self, ctx, *args):
        """Simple unary echo (return args)."""
        self.log.info('echo_args called %s', ctx)
        return args

    @prpc.method
    async def echo_kwargs(self, ctx, **kwargs):
        """Simple unary echo (return kwargs)."""
        self.log.info('echo_kwargs called %s', ctx)
        return kwargs

    @prpc.method
    async def echo_stream(self, ctx, retval=True):
        """Trendy bistream echo (echoes stream messages until stream is closed).
        """
        self.log.info('echo_stream called %s %s', ctx, ctx.stream)
        async for msg in ctx.stream:
            await ctx.stream.send(msg)
        return retval

    @prpc.method
    async def echo_callback(self, ctx, callback_name, *args, **kwargs):
        """Really fancy callback echo - runs an RPC call against
        caller with the same args.
        """
        self.log.info('echo_callback called %s', ctx)
        await ctx.connection.call_simple(callback_name, *args, **kwargs)

    @prpc.method(expand_args=False)
    async def echo_adaptive(self, ctx):
        """Cunning echo - behaves differently depending on call type."""
        self.log.info('echo_adaptive called %s', ctx)
        assert ctx.call_type in prpc.CallType
        if ctx.call_type == prpc.CallType.UNARY:
            return ctx.args
        elif ctx.call_type == prpc.CallType.ISTREAM:
            await ctx.stream.send(ctx.args)
        elif ctx.call_type == prpc.CallType.OSTREAM:
            return await ctx.stream.receive()
        elif ctx.call_type == prpc.CallType.BISTREAM:
            await ctx.stream.send(await ctx.stream.receive())
