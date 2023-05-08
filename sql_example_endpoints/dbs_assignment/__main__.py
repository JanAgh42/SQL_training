from dbs_assignment.main_router import main_router
from fastapi import FastAPI

app = FastAPI(title = "DBS_database")
app.include_router(main_router)