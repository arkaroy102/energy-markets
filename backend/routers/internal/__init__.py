import os
from fastapi import APIRouter, Depends, Header, HTTPException

INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY")


def verify_internal_caller(x_internal_key: str = Header(...)):
    if not INTERNAL_API_KEY:
        raise HTTPException(status_code=500, detail="INTERNAL_API_KEY not configured")
    if x_internal_key != INTERNAL_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


internal_router = APIRouter(
    prefix="/internal",
    dependencies=[Depends(verify_internal_caller)],
)
