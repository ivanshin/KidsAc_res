from pydantic import BaseModel, HttpUrl

class CallbackRequest(BaseModel):
    callback_url : HttpUrl