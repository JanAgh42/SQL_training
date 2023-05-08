from dbs_assignment.api_endpoints.window_functions import window_functions_router
from dbs_assignment.api_endpoints.flights import  flights_router
from fastapi import APIRouter

main_router = APIRouter()
main_router.include_router(flights_router, tags = ["flights"])
main_router.include_router(window_functions_router, tags = ["window_functions"])