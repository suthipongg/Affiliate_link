import functools
from fastapi import HTTPException, status
from configs.logger import logger

def handle_exceptions(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except HTTPException as http_exception:
            logger.error(http_exception)
            raise http_exception
        except Exception as e:
            logger.error(e)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Internal Server Error : {e}") from e  
    return wrapper