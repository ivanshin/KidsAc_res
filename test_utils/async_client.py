import asyncio
import httpx
import time



async def main():

    async with httpx.AsyncClient(timeout= None) as client:
        start_time = time.time()
        for number in range(1, 5):

            url = f'http://127.0.0.1:8000/predictions'
            resp = await client.post(url, json= {"callback_url": "http://127.0.0.1:9000/callback_emul"})
            #assert resp.status_code != 200, f"Wrong status code {resp.status_code}"
        
        print("--- %s seconds ---" % (time.time() - start_time))

asyncio.run(main())
