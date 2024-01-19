from fastapi import FastAPI
from pydantic import BaseModel
import dispatch_logic
import dispatch_logic_imminent

app = FastAPI()

class TestBody(BaseModel):
    idx: int
    imminent: int

@app.get("/")
def root():
    return {"message": "Hello World"}

@app.post("/reservation")
def echo(msg: TestBody):

    idx = msg.idx
    imminent = msg.imminent
    if imminent == 0:

        total_list = dispatch_logic.run(idx)
    else:
        total_list = dispatch_logic_imminent.run(idx)

    return total_list



