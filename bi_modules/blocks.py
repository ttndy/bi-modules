from prefect.blocks.core import Block,SecretStr,SecretDict
from typing import Dict, Optional
from prefect.blocks.system import Secret, JSON

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
    system_secrets: Optional[SecretDict] = None
    system_variables: Optional[Dict] = None



if __name__ == '__main__':
    system_configuration_block = SystemConfiguration.load("datateam-email-credentials")
    system_secrets = system_configuration_block.system_secrets.get_secret_value()
    print(system_secrets)
