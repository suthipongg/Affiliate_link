from fastapi import status, HTTPException, Depends
from pydantic import BaseModel
from fastapi.security.http import HTTPAuthorizationCredentials, HTTPBearer
import typing as t
import os

# Placeholder for a database containing valid token values
known_tokens = os.getenv('API_TOKEN', 'thisisafastapifrommgbproject2023')

# We will handle a missing token ourselves
get_bearer_token = HTTPBearer(auto_error=False)

class UnauthorizedMessage(BaseModel):
    detail: str = "Bearer token missing or unknown"


async def get_token(
    auth: t.Optional[HTTPAuthorizationCredentials] = Depends(get_bearer_token),
) -> str:
    # Simulate a database query to find a known token
    if auth is None or (token := auth.credentials) != known_tokens:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=UnauthorizedMessage().detail,
        )
    return token