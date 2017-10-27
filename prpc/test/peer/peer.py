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

import argparse
import asyncio
import logging
import pathlib
import signal
import socket
import sys

import aiohttp
import aiohttp.web

sys.path.append(str(pathlib.Path(__file__).absolute().parents[3]))
import prpc


def setup_logging():
    root_logger = logging.getLogger()
    # root logger accepts all messages, filter on handlers level
    root_logger.setLevel(logging.DEBUG)
    for handler in [logging.StreamHandler(sys.stdout)]:
        handler.setFormatter(
            logging.Formatter(
                '[%(process)d] [%(asctime)s] '
                '[%(name)s] [%(levelname)s] %(message)s'
            )
        )
        root_logger.addHandler(handler)


def on_terminate(sig_num, frame):
    sys.stdout.flush()
    sys.stderr.flush()
    asyncio.get_event_loop().stop()


async def rpc_server_handler(request):
    connection = prpc.Connection(
        request.app['service'],
        logger=logging.getLogger('ServerConnection'),
        debug=True
    )
    return await prpc.platform.ws_aiohttp.accept(connection, request)


def main():
    signal.signal(signal.SIGTERM, on_terminate)
    setup_logging()

    parser = argparse.ArgumentParser(description='RPC test peer')
    parser.add_argument('--connect', type=str, help='endpoint to connect to')
    parser.add_argument(
      'service', type=str,
      help='service definition .py file'
    )

    args = parser.parse_args()

    service = getattr(__import__(args.service), 'Service')()

    if args.connect:
        raise NotImplementedError('client mode - not yet')

    app = aiohttp.web.Application()
    app['service'] = service
    app.router.add_get('/', rpc_server_handler)

    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    # Aiohttp will call 'listen' inside.
    # But it must be called before we print out the port number.
    sock.listen(128)
    _, port = sock.getsockname()
    print('Port: %d' % (port,))

    aiohttp.web.run_app(app, sock=sock)


if __name__ == '__main__':
    main()
