from prefect.blocks.core import Block
from prefect.blocks.fields import SecretDict
from typing import Dict, Optional
from prefect.blocks.system import Secret, JSON
from prefect_snowflake import SnowflakeCredentials

from pydantic.v1 import  SecretStr


class Snowflake_Custom_Credentials(Block):
    _logo_url = SnowflakeCredentials._logo_url
    _block_type_name = 'Snowflake Custom Credentials'
    _description = 'A more simplified version of the Snowflake Credentials block. The values stored in this block will be obfuscated when this block is logged or shown in the UI.'
    account: SecretStr
    user: SecretStr
    password: SecretStr


class Basic_Credentials(Block):
    _logo_url = Secret._logo_url
    _block_type_name = 'Basic Credentials'
    _description = 'A block that stores a username and password for basic authentication. The values stored in this block will be obfuscated when this block is logged or shown in the UI.'
    username: SecretStr
    password: SecretStr


class SystemConfiguration(Block):
    _logo_url = JSON._logo_url
    _block_type_name = 'System Configuration'
    _description = 'A configuration block for storing configuration secrets and variables.'
    system_secrets: Optional[SecretDict]
    system_variables: Optional[Dict]



if __name__ == '__main__':
    system_configuration_block = Basic_Credentials.load("sf-old-key")
    system_secrets = system_configuration_block.username.get_secret_value()
    print(system_secrets)
