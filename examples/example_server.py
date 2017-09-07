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
