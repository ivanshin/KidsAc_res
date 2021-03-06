from http import HTTPStatus
import requests
from fastapi import APIRouter, FastAPI
import asyncio

from layers.model_layer import make_predicts_pipeline
from layers.data_preproc_layer import main_preproc_pipeline
from layers.config_layer import get_path_to_db_tbs, get_path_to_pp_tbs

from entity_models.callback_request_model import CallbackRequest



PATH_TO_DB_TABLES = get_path_to_db_tbs()
PATH_TO_PREPROCCESED_TABLES = get_path_to_pp_tbs()


app = FastAPI(title= 'Churn prediction API', version= '0.0.1', openapi_url= '/openapi.json')


api_router = APIRouter()


@api_router.get("/", status_code= 200)
def root() -> dict:

    """ Root get """

    return {"message" : "root"}


@api_router.post("/signup")
def signup():
    return


@api_router.post("/login")
def login():
    return


@api_router.get("/refresh_token")
def refresh_token():
    return


@app.post('/secret')
def secret_data():
    return 'Secret data'


@app.get('/notsecret')
def not_secret_data():
    return 'Not secret data'


async def emul_procces(arg):
    emulate_res = {'1': []}
    for i in range(0, 10000000):
        emulate_res['1'].append(i)
    
    req = requests.post(url= arg.callback_url, json= emulate_res, timeout= None)

    return


@api_router.post("/predictions")
async def make_predictions(arg: CallbackRequest):

    """ Get user ID's of possible churn and send to callback url"""
    
    #await main_preproc_pipeline('')
    #results = await make_predicts_pipeline(path_to_preproccesed_table= 'tmp_data')

    asyncio.create_task(emul_procces(arg))
    

    #requests.post(url= arg.callback_url, json= results, timeout= None)

    return HTTPStatus.OK

    #req = requests.post(url= arg.callback_url, json= results, timeout= None)

    #return req.status_code
    

app.include_router(api_router)
