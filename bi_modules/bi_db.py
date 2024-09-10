from sqlalchemy import create_engine
import snowflake.connector
from prefect.blocks.system import String
env = String.load("environment").value
from prefect_snowflake import SnowflakeCredentials
from prefect import runtime
################################################################################################################################

class SnowflakeConnection:
    def __init__(self, account, database, schema, username, password, warehouse, role, tag):
        self.account = account
        self.database = database
        self.schema = schema
        self.username = username
        self.password = password
        self.warehouse = warehouse
        self.role = role
        self.tag = tag

    def connect(self):

        engine = snowflake.connector.connect(
                                            user=self.username,
                                            password=self.password,
                                            account=self.account,
                                            database=self.database,
                                            schema=self.schema,
                                            warehouse=self.warehouse,
                                            role=self.role,
                                            query_tag=self.tag
                        )


        return engine
    
################################################################################################################################


def sf_pe_prod_connection(database: str = 'BUSINESSINTEL01'
                          ,schema: str = 'INFORMATION_SCHEMA'
                          ,role: str = 'DEV_DATA_ENG_FR_AM'
                          ,warehouse: str = 'PROD_INGESTION_DE_WH'
                          ,sql_alchemy = False
                          ,tag = f'F:{runtime.flow_run.name} - D:{runtime.deployment.name} - T:{runtime.task_run.name}'
                          ):
    
    snowflake_credentials_block = SnowflakeCredentials.load("snowflake-data-engineering-etl")
    username = snowflake_credentials_block.user
    password = snowflake_credentials_block.password.get_secret_value()
    account = snowflake_credentials_block.account

    if env == 'QA':
        role = 'ACCOUNTADMIN'
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
                    role=role,
                    tag=tag
                )
    snowflake.connector.paramstyle = 'qmark'
    return connection.connect()


if __name__ == "__main__":
    print(env)
