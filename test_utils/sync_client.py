import httpx
import time


client = httpx.Client(timeout= None)

start_time = time.time()
for number in range(1, 2):
    url = f'http://127.0.0.1:8000/predictions'
    resp = client.get(url)
    #assert resp.status_code != 200, f"Wrong status code {resp.status_code}"

print("--- %s seconds ---" % (time.time() - start_time))