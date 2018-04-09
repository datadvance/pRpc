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
import pathlib
import sys
import uuid

import prpc


class Service(object):
    log = logging.getLogger('StreamsService')

    @prpc.method
    async def endless_stream(self, ctx):
        self.log.info('endless_stream called %s', ctx)
        MSG_DELAY = 0.01
        while True:
            await ctx.stream.send(uuid.uuid4().bytes)
            # Don't send messages at max speed to avoid multiple
            # warning from network layer
            # (closing the socket is not 'synchronous enough' in aiohttp?).
            await asyncio.sleep(MSG_DELAY, loop=ctx.loop)

    @prpc.method
    async def pseudodownload(self, ctx, chunks=128):
        self.log.info('pseudodownload called %s', ctx)
        CLOSE_DELAY = 0.1
        for _ in range(chunks):
            await ctx.stream.send(uuid.uuid4().bytes)
        await ctx.stream.close()
        await asyncio.sleep(CLOSE_DELAY, loop=ctx.loop)
        return chunks

    @prpc.method
    async def counter(self, ctx):
        self.log.info('counter called %s', ctx)
        counter = 0
        async for msg in ctx.stream:
            counter += 1
        return counter
