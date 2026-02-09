from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from project.utils import get_env_var_config
from snowflake.snowpark.session import Session
from pydantic import BaseModel
from typing import List, Optional

app = FastAPI(title="Weather Query Service", version="1.0.0")

# Initialize Snowflake session
session = None


def init_session():
    """Initialize Snowflake session on startup"""
    global session
    session = Session.builder.configs(get_env_var_config()).create()



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
