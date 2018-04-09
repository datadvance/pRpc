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

import prpc.exceptions as exceptions
import prpc.protocol as protocol
from prpc.protocol.constants import StreamMode
from prpc.stream import Stream


def test_open(event_loop, test_log):
    """Check stream open modes."""
    test_log.info('Opening a read-only stream')
    stream = Stream('id_unused', event_loop)
    assert stream.is_closed
    assert not stream.is_open
    stream.open(StreamMode.READ)
    assert stream.is_readable
    assert not stream.is_writable
    assert not stream.is_closed
    assert stream.is_open

    test_log.info('Opening a write-only stream')
    stream = Stream('id_unused', event_loop)
    stream.open(StreamMode.WRITE)
    assert not stream.is_readable
    assert stream.is_writable

    test_log.info('Opening a read-write stream')
    stream = Stream('id_unused', event_loop)
    stream.open(StreamMode.READ | StreamMode.WRITE)
    assert stream.is_readable
    assert stream.is_writable

    test_log.info('Trying wrong input type')
    stream = Stream('id_unused', event_loop)
    with pytest.raises(TypeError):
        stream.open(2)

    test_log.info('Trying a NULL mode')
    stream = Stream('id_unused', event_loop)
    with pytest.raises(exceptions.InvalidStreamMode):
        stream.open(StreamMode.READ &
                    StreamMode.WRITE)

    test_log.info('Double open should raise')
    stream = Stream('id_unused', event_loop)
    stream.open(StreamMode.READ)
    with pytest.raises(exceptions.InvalidStreamState):
        stream.open(StreamMode.READ)

    test_log.info('Reopen should raise')
    stream = Stream('id_unused', event_loop)
    stream.open(StreamMode.READ)
    stream.close_sync()
    with pytest.raises(exceptions.InvalidStreamState):
        stream.open(StreamMode.READ)


@pytest.mark.async_test
async def test_feed_receive(event_loop, test_log):
    """Check feed-receive scenarios used in the library."""
    # 'positive' scenario tested for R and RW streams
    for stream_mode in [StreamMode.READ, StreamMode.READ | StreamMode.WRITE]:
        test_log.info('Testing mode %s', stream_mode.name)
        stream = Stream('id_unused', event_loop)
        stream.open(stream_mode)
        test_log.info('Feeding stream until buffer is full')
        assert not stream.data_available
        with pytest.raises(asyncio.TimeoutError):
            await stream.receive(timeout=0)

        for i in range(protocol.constants.STREAM_BUFFER_QUEUE_SIZE):
            assert await stream.feed('message #%d' % (i,))
        assert stream.data_available
        test_log.info('Now feed should timeout')
        assert not await stream.feed('does not fit', 0)
        assert not await stream.feed('does not fit either', 0.1)
        await stream.receive()
        assert await stream.feed('fits again', 0)
        test_log.info('Test that it is well-behaved after close')
        stream.close_sync()
        assert not await stream.feed('stream is closed already')
        test_log.info('Read all messages back and check count')
        # [msg async for msg in stream] breaks the IDE xD
        message = await stream.receive()
        messages = []
        while message is not None:
            messages.append(message)
            message = await stream.receive()
        assert (len(messages) == protocol.constants.STREAM_BUFFER_QUEUE_SIZE)
        test_log.info('Now recieve should fail')
        with pytest.raises(exceptions.InvalidStreamState):
            await stream.receive()

    test_log.info('Feeding write-only streams should fail (miserably)')
    stream = Stream('id_unused', event_loop)
    stream.open(StreamMode.WRITE)
    assert not stream.data_available
    with pytest.raises(exceptions.InvalidStreamMode):
        await stream.feed('whatever')
    with pytest.raises(exceptions.InvalidStreamMode):
        await stream.receive('whatever')

    test_log.info('Feeding closed streams returns False')
    stream = Stream('id_unused', event_loop)
    assert not await stream.feed('this message is silently dropeed')


@pytest.mark.async_test
async def test_send_write(event_loop):
    """Check feed-receive scenarios used in the library."""
    STREAM_ID = 'whatever'
    DATA = b'data'

    def make_writer():
        queue = asyncio.Queue()

        async def writer(id, data):
            assert id == STREAM_ID
            await queue.put(data)
        return writer, queue

    for stream_mode in [StreamMode.WRITE, StreamMode.READ | StreamMode.WRITE]:
        stream = Stream(STREAM_ID, event_loop)
        writer, queue = make_writer()
        stream.on_write.append(writer)
        with pytest.raises(exceptions.InvalidStreamMode):
            await stream.send(None)
        stream.open(stream_mode)
        assert stream.is_writable
        await stream.send(DATA)
        assert await queue.get() == DATA
        with pytest.raises(TypeError):
            await stream.send(None)
        stream.close_sync()
        with pytest.raises(exceptions.InvalidStreamState):
            await stream.send(None)


@pytest.mark.async_test
async def test_close(event_loop):
    """Check that close signal is emitted."""
    STREAM_ID = 'whatever'
    CLOSE_TAG = 'closed'

    def make_closer():
        queue = asyncio.Queue()

        async def closer(id):
            assert id == STREAM_ID
            await queue.put(CLOSE_TAG)
        return closer, queue

    for stream_mode in [
        StreamMode.READ, StreamMode.WRITE, StreamMode.READ | StreamMode.WRITE
    ]:
        stream = Stream(STREAM_ID, event_loop)
        closer, queue = make_closer()
        stream.on_close.append(closer)
        stream.open(stream_mode)
        await stream.close()
        assert await queue.get() == CLOSE_TAG
