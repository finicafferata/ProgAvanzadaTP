from fastapi import FastAPI
import pandas as pd
import psycopg2


app = FastAPI()

@app.get("/recommendations/{ADV}/{Modelo}")
def get_recommendations(ADV:str,Modelo:str):
    

    return {"message": "Hello World"}
