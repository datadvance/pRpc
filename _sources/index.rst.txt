*****
prpc
*****

Quick start
===========

Simple server::

    import aiohttp.web

    import prpc

    class Service(object):
        @prpc.method
        async def foo(self, ctx):
            request = await ctx.stream.receive()
            assert request == "does it work?"
            await ctx.stream.send("it works!")
            return True

    async def rpc_handler(request):
        connection = prpc.Connection(Service())
        return await prpc.platform.ws_aiohttp.accept(connection, request)

    app = aiohttp.web.Application()
    app.router.add_get("/rpc", rpc_handler)
    aiohttp.web.run_app(app, port=51337)

Note that all RPC methods must accept :class:`~prpc.call_context.CallContext`
object as a first argument.

Simple client::

  import asyncio

  import prpc

  async def main():
      connection = prpc.Connection(None)
      await prpc.platform.ws_aiohttp.connect(
          connection, "ws://127.0.0.1:51337/rpc"
      )
      print("Connected!")
      async with connection.call_bistream("foo") as call:
          await call.stream.send("does it work?")
          response = await call.stream.receive()
          assert response == "it works!"
          result = await call.result
          print("Call result:", result)
          assert result
      await connection.close()

  asyncio.get_event_loop().run_until_complete(main())


Contents
========

.. toctree::
   :maxdepth: 2

   reference/index



:ref:`search`
