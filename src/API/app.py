from fastapi import FastAPI
from routes.sales_API import router


app =FastAPI()


app.include_router(router)
