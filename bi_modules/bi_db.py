from sqlalchemy import create_engine
import snowflake.connector
from prefect.blocks.system import String
env = String.load("environment").value
from prefect_snowflake import SnowflakeCredentials

################################################################################################################################

class SnowflakeConnection:
    def __init__(self, account, database, schema, username, password, warehouse, role):
        self.account = account
        self.database = database
        self.schema = schema
        self.username = username
        self.password = password
        self.warehouse = warehouse
        self.role = role

    def connect(self):

        engine = snowflake.connector.connect(
                                            user=self.username,
                                            password=self.password,
                                            account=self.account,
                                            database=self.database,
                                            schema=self.schema,
                                            warehouse=self.warehouse,
                                            role=self.role
                        )


        return engine
    
################################################################################################################################


def sf_pe_prod_connection(database: str = 'BUSINESSINTEL01'
                          ,schema: str = 'INFORMATION_SCHEMA'
                          ,role: str = 'DEV_DATA_ENG_FR_AM'
                          ,warehouse: str = 'PROD_INGESTION_DE_WH'
                          ,sql_alchemy = False
                          ):
    
    snowflake_credentials_block = SnowflakeCredentials.load("snowflake-data-engineering-etl")
    username = snowflake_credentials_block.user
    password = snowflake_credentials_block.password.get_secret_value()
    account = snowflake_credentials_block.account
    role = snowflake_credentials_block.role

    if env == 'QA':
        role = 'DEV_DATA_ENG_FR_AM'
        database = database + '_QA'
        
    if sql_alchemy:
        engine = create_engine(f'snowflake://{username}:{password}@{account}/{database}/{schema}?role={role}&warehouse={warehouse}')
        return engine
    
    connection = SnowflakeConnection(
                    account=account,
                    database=database,
                    schema=schema,
                    username=username,
                    password=password,
                    warehouse=warehouse,
                    role=role
                )
    snowflake.connector.paramstyle = 'qmark'
    return connection.connect()


if __name__ == "__main__":
    print(env)
