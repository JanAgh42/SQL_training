from pydantic import BaseSettings

class Configuration(BaseSettings):

    DATABASE_HOST: str
    DATABASE_PORT: str
    DATABASE_NAME: str
    DATABASE_USER: str
    DATABASE_PASSWORD: str

    class Config:
        env_file = '.env'
        case_sensitive = True

api_configuration = Configuration()