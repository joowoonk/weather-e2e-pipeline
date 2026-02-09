from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from project.utils import get_env_var_config
from snowflake.snowpark.session import Session
from pydantic import BaseModel
from typing import List, Optional
import uvicorn

app = FastAPI(title="Weather Query Service", version="1.0.0")

# Initialize Snowflake session
session = None


def init_session():
    """Initialize Snowflake session on startup"""
    global session
    session = Session.builder.configs(get_env_var_config()).create()


@app.on_event("startup")
async def startup_event():
    init_session()

@app.on_event("shutdown")
async def shutdown_event():
    if session:
        session.close()

class WeatherQuery(BaseModel):
    city_name: Optional[str] = None
    observation_date: Optional[str] = None  # Format: 'YYYY-MM-DD'

@app.post("/query_weather")
async def query_weather(query: WeatherQuery):
    """
    Query the WEATHER_DAILY_AGGREGATIONS table based on city name and/or observation date.
    """
    if not session:
        raise HTTPException(status_code=500, detail="Snowflake session not initialized")

    try:
        df = session.table("WEATHER_DB.PUBLIC.WEATHER_DAILY_AGGREGATIONS")

        if query.city_name:
            df = df.filter(df["CITY_NAME"] == query.city_name)
        if query.observation_date:
            df = df.filter(df["OBSERVATION_DATE"] == query.observation_date)

        results = df.collect()
        return JSONResponse(content=[row.as_dict() for row in results])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
