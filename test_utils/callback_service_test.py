from fastapi import FastAPI
import json
from pydantic import BaseModel


app = FastAPI(title= "Callback_test")


#class Results(BaseModel):
#    user_id: dict


@app.post("/callback")
async def post_results(data : dict):
    with open('data.json', 'w') as fp:
        json.dump(data, fp)


@app.post("/callback_emul")
async def post_results(data : dict):
    with open('data_test.json', 'w') as fp:
        json.dump(data, fp)


