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

from . import asyncio as aioutils


class Signal(list):
    """Extremely simple 'pub-sub' implementation inspired by aiohttp.

    Client API is inherited from the built-in list class,
    to add a listener one can just use *append*.

    Accepts both sync and async callables as listeners.

    Example::

      signal = Signal()
      signal.append(sync_listener)
      signal.append(async_listener)
      await signal.send("arg")

    """
    __slots__ = ()

    async def send(self, *args, **kwargs):
        """Emit the signal passing arbitrary arguments to all subscribers."""
        # Make a shallow copy of the list as subscribers may try
        # to edit the subscription. For realistic example, see
        # platform.ws_aiohttp.connect.
        for subscriber in list(self):
            # TODO: maybe handle exceptions for each subscriber
            # (so one fail doesn't interrupt everything)?
            await aioutils.call_sync_async(subscriber, *args, **kwargs)
