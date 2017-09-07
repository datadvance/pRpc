import asyncio

import prpc


async def main():
    connection = prpc.Connection(None)
    await prpc.platform.ws_aiohttp.connect(connection, "ws://127.0.0.1:51337/rpc")
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
