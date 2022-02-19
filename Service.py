import requests
from fastapi import APIRouter, FastAPI, Depends
from layers.model_layer import make_predicts_pipeline
from layers.data_preproc_layer import main_preproc_pipeline
from entity_models.callback_request_model import CallbackRequest


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


@api_router.post("/predictions")
async def make_predictions(arg: CallbackRequest):

    """ Get user ID's of possible churn and send to callback url"""
    
    #await main_preproc_pipeline('')
    results = await make_predicts_pipeline(path_to_preproccesed_table= 'tmp_data')

    req = requests.post(url= arg.callback_url, json= results, timeout= None)

    return req.status_code
    #return results
    

app.include_router(api_router)
