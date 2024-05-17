import asyncio
import datetime
import inspect
import json
import time
from pathlib import Path
from typing import List, Optional

import pandas as pd
import requests
import snowflake.connector
import yaml
from O365 import Account
from prefect.blocks.system import JSON, Secret, String
from sqlalchemy import create_engine

try:
    from .blocks import Basic_Credentials, SystemConfiguration
except ImportError:
    from blocks import Basic_Credentials, SystemConfiguration

# Load environment and system secrets asynchronously
async def load_env_and_secrets():
    env_block = await String.load("environment")
    env = env_block.value
    system_configuration_block = await SystemConfiguration.load("datateam-email-credentials")
    system_secrets = system_configuration_block.system_secrets.get_secret_value()
    return env, system_secrets

# Utility functions to find paths
def find_yaml_path():
    return 'recipients.yaml'

def find_reports_yaml_path():
    return 'reports.yaml'

# Login function for O365
async def login(system_secrets) -> Account:
    main_resource_block = await String.load("datateam-email")
    main_resource = main_resource_block.value
    account = Account(
        credentials=(system_secrets['client_id'], system_secrets['client_secret']),
        auth_flow_type='credentials',
        tenant_id=system_secrets['tenant_id'],
        main_resource=main_resource
    )
    if not account.authenticate():
        raise Exception('Authentication Error')
    print('Authenticated')
    return account

# Email reading function
async def read_inbox(system_secrets) -> None:
    account = await login(system_secrets)
    mail = account.mailbox()
    inbox = mail.inbox_folder()
    async for message in inbox.get_messages():
        print(message)

# PowerBiRefresh class
class PowerBiRefresh:
    def __init__(self, report_name: str, group_name: str, number_of_tries: int = 5):
        self.report_name = report_name
        self.group_name = group_name
        self.number_of_tries = number_of_tries
        self.api_token, self.start_time, self.expires_in = asyncio.run(self.get_power_bi_access_token())
        self.group_id = asyncio.run(self.get_group_id())
        self.dataset_id = asyncio.run(self.get_dataset_id())
        self.report_id, self.report_url = asyncio.run(self.get_report_id())

    async def get_power_bi_access_token(self):
        pbi_api_uid = await Secret.load("pbi-api-uid")
        pbi_api_pwd = await Secret.load("pbi-api-pwd")
        pbi_api_cid = await Secret.load("pbi-api-cid")
        pbi_api_cse = await Secret.load("pbi-api-cse")

        url = "https://login.microsoftonline.com/common/oauth2/token"
        payload = {
            "username": await pbi_api_uid.get(),
            "password": await pbi_api_pwd.get(),
            "client_id": await pbi_api_cid.get(),
            "client_secret": await pbi_api_cse.get(),
            "resource": "https://analysis.windows.net/powerbi/api",
            "grant_type": "password",
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        for _ in range(6):
            try:
                response = requests.post(url, headers=headers, data=payload)
                response.raise_for_status()
                api_token = "Bearer " + response.json()["access_token"]
                expires_in = int(response.json()['expires_in'])
                return api_token, datetime.datetime.now(), expires_in
            except Exception as e:
                print('Error: \n' + str(e))
                await asyncio.sleep(60)
        raise Exception("Failed to get Power BI access token")

    async def check_token_refresh(self):
        if (datetime.datetime.now() - self.start_time).seconds > self.expires_in - 60:
            self.api_token, self.start_time, self.expires_in = await self.get_power_bi_access_token()

    async def get_group_id(self):
        await self.check_token_refresh()
        url = "https://api.powerbi.com/v1.0/myorg/groups"
        headers = {"Authorization": self.api_token}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        groups = response.json()['value']
        return next((group['id'] for group in groups if group['name'].lower() == self.group_name.lower()), None)

    async def get_dataset_id(self):
        await self.check_token_refresh()
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.group_id}/datasets"
        headers = {"Authorization": self.api_token}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        datasets = response.json()['value']
        return next((dataset['id'] for dataset in datasets if dataset['name'] == self.report_name), None)

    async def refresh_power_bi_dataset(self):
        await self.check_token_refresh()
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.group_id}/datasets/{self.dataset_id}/refreshes"
        headers = {"Authorization": self.api_token}
        for _ in range(5):
            try:
                response = requests.post(url, headers=headers)
                response.raise_for_status()
                await asyncio.sleep(10)
                return response
            except Exception as e:
                print(f'Power BI API refresh error: {e}')
                await asyncio.sleep(10)
        raise Exception("Failed to refresh Power BI dataset")

    async def power_bi_dataset_refresh_status(self):
        await self.check_token_refresh()
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.group_id}/datasets/{self.dataset_id}/refreshes?$top=1"
        headers = {"Authorization": self.api_token}
        response = requests.get(url, headers=headers)
        if response.status_code != 401:
            response.raise_for_status()
            return response.json()['value'][0]['status']
        return None

    async def power_bi_check_refresh_status(self):
        await self.check_token_refresh()
        for i in range(self.number_of_tries):
            status = await self.power_bi_dataset_refresh_status()
            if status == 'Completed':
                print('Status Check: Completed')
                return status
            print(f"Status Check: Not completed, checking again in {i + 1} minute(s)")
            await asyncio.sleep((i + 1) * 60)
        raise TimeoutError("Power BI refresh did not complete in time")

    async def trigger_and_check_refresh(self):
        await self.check_token_refresh()
        refresh_response = await self.refresh_power_bi_dataset()
        if refresh_response.status_code == 202:
            print("Refresh Response: Success, refresh is in progress.")
        elif refresh_response.status_code == 400:
            print(f"Refresh Response: A refresh for dataset belonging to {self.report_name} is already in progress.")
        else:
            print(f"Refresh Response: Failed, received status code: {refresh_response.status_code}")
        status_check_response = await self.power_bi_check_refresh_status()
        print("Status Check Response: ", status_check_response)
        return refresh_response, status_check_response

    async def get_report_id(self):
        await self.check_token_refresh()
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.group_id}/reports"
        headers = {"Authorization": self.api_token}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        reports = response.json().get('value', [])
        for report in reports:
            if report['datasetId'] == self.dataset_id:
                print('Report Name: ', report['name'])
                return report['id'], report['webUrl']
        return None, None

    async def export_report_to_file(self, format_type="PPTX"):
        await self.check_token_refresh()
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.group_id}/reports/{self.report_id}/ExportTo"
        headers = {"Authorization": self.api_token, "Content-Type": "application/json"}
        body = {"format": format_type}
        response = requests.post(url, headers=headers, json=body)
        response.raise_for_status()
        self.export_id = response.json()['id']
        return self.export_id

    async def check_export_status(self):
        await self.check_token_refresh()
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.group_id}/reports/{self.report_id}/exports/{self.export_id}"
        headers = {"Authorization": self.api_token}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        self.export_status = response.json()['status']
        return self.export_status

    async def get_export_file(self):
        await self.check_token_refresh()
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.group_id}/reports/{self.report_id}/exports/{self.export_id}/file"
        headers = {"Authorization": self.api_token}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        self.export_file = response.content
        return self.export_file

    async def export_report(self, format_type="PPTX"):
        await self.check_token_refresh()
        await self.export_report_to_file(format_type)
        while (status := await self.check_export_status()) != "Succeeded":
            await asyncio.sleep(5)
        file_content = await self.get_export_file()
        self.report_export_file_name = f"{self.report_name}.{format_type.lower()}"
        with open(self.report_export_file_name, "wb") as f:
            f.write(file_content)
        print("Report exported successfully.")
        return self.report_export_file_name

    async def pbi_refresh(self):
        await self.check_token_refresh()
        refresh_response = await self.refresh_power_bi_dataset()
        if refresh_response.status_code == 202:
            print("Refresh Response: Success, refresh is in progress.")
        else:
            print(f"Refresh Response: Failed, received status code: {refresh_response.status_code}")
        status_check_response = await self.power_bi_check_refresh_status()
        print("Status Check Response: ", status_check_response)

# SnowflakeConnection class
class SnowflakeConnection:
    def __init__(self, account, database, schema, username, password, warehouse, role):
        self.account = account
        self.database = database
        self.schema = schema
        self.username = username
        self.password = password
        self.warehouse = warehouse
        self.role = role

    async def connect(self):
        return snowflake.connector.connect(
            user=self.username,
            password=self.password,
            account=self.account,
            database=self.database,
            schema=self.schema,
            warehouse=self.warehouse,
            role=self.role
        )

# Send email function
async def send_email(
    recipient_yaml_file_path: str = find_yaml_path(),
    subject: str = 'test',
    body: str = 'test',
    attachments: List[str] = [],
    message_status: Optional[str] = None,
    email: str = None
) -> None:
    allowable_statuses = ["Success", "Error", "Warning"]
    if message_status and message_status not in allowable_statuses:
        raise ValueError(f"message_status must be one of {allowable_statuses}, got '{message_status}' instead.")
    print(f"Message Status: {message_status}")

    env, system_secrets = await load_env_and_secrets()
    account = await login(system_secrets)
    with open(recipient_yaml_file_path, 'r') as file:
        load_yaml = yaml.safe_load(file)
        recipients = load_yaml['recipients']
        if message_status == "Error":
            recipients += load_yaml.get('error_recipients', [])
        if email:
            recipients.append(email)

    m = account.new_message()
    if env == 'QA':
        main_resource_block = await String.load("datateam-email")
        main_resource =  main_resource_block.value
        recipients = [main_resource]
        if message_status == 'Error':
            recipients += load_yaml.get('error_recipients', [])
        if email:
            recipients.append(email)
        subject += ' - DEBUG MODE'

    m.to.add(recipients)
    m.subject = subject

    body_templates = {
        'Success': f"""
            <div style="font-family: Arial, sans-serif; border: 2px solid #4CAF50; padding: 16px; border-radius: 8px; background-color: #DFF2BF;">
                <h2 style="color: #4CAF50;">Success:</h2>
                <h3 style="color: #4CAF50;">Log Contents:</h3>
                <div style="overflow-x: auto;">{body}</div>
                <br>
            </div>
        """,
        'Error': f"""
            <div style="font-family: Arial, sans-serif; border: 2px solid #FF0000; padding: 16px; border-radius: 8px; background-color: #FFCFCF;">
                <h2 style="color: #FF0000;">Error Log Contents:</h2>
                <h4 style="overflow-x: auto;">{body}</h4>
            </div>
        """,
        'Warning': f"""
            <div style="font-family: Arial, sans-serif; border: 2px solid #FFC107; padding: 16px; border-radius: 8px; background-color: #FFF9C4;">
                <h2 style="color: #FFC107;">Warning:</h2>
                <p style="font-size: 18px;">{body}</p>
            </div>
        """
    }
    m.body = body_templates.get(message_status, body)

    for attachment in attachments:
        m.attachments.add(attachment)
    m.send()

# Report refresh function
async def report_refresh(
    number_of_tries: int = 5,
    send_email_when_done: bool = False,
    subject: str = '',
    body: str = '',
    recipient_yaml_file_path: str = find_yaml_path(),
    reports_yaml_file_path: str = find_reports_yaml_path()
) -> None:
    with open(reports_yaml_file_path, 'r') as file:
        reports = yaml.safe_load(file).get('reports', [])
        
        for report in reports:
            report_name = report.get('name')
            group_name = report.get('group_name')
            refresh = report.get('Refresh', True)
            export_options = report.get('Export', [])
            
            # Convert list of dictionaries into a single dictionary for easier access
            export_options_dict = {k: v for d in export_options for k, v in d.items()}
            
            pdf = export_options_dict.get('PDF', False)
            png = export_options_dict.get('PNG', False)
            pptx = export_options_dict.get('PPTX', False)

            if refresh:
                print(f"Refreshing {report_name} in Workspace {group_name}...")
                power_bi_refresh = PowerBiRefresh(report_name, group_name, number_of_tries)  
                await power_bi_refresh.pbi_refresh()

                if send_email_when_done:  
                    report_id, webUrl = await power_bi_refresh.get_report_id()

                    html_content = f'''
                    <div style="font-family: Arial, sans-serif; border: 2px solid #4CAF50; padding: 16px; border-radius: 8px; background-color: #f9f9f9;">
                        <h2 style="color: #4CAF50;">Power BI Refresh: <span style="font-weight: bold;">{report_name} in Workspace: {group_name}</span> completed successfully.</h2>
                        <p style="font-size: 18px;">Click the button below to view the report:</p>
                        <br><a href="{webUrl}" style="background-color: #4CAF50; color: white; padding: 14px 20px; margin: 8px 0; border: none; cursor: pointer; border-radius: 4px; text-decoration: none;">Link to Report</a><br>
                    </div>
                    '''

                    body = html_content
                    subject = f"Power BI Refresh: {report_name} in workspace {group_name} completed successfully."
                
                    files = []

                    if pptx:
                        if report_id is not None:
                            files += [await power_bi_refresh.export_report("PPTX")]
                        else:
                            print("No report found for the dataset.")
                    if png:
                        if report_id is not None:
                            files += [await power_bi_refresh.export_report("PNG")]
                        else:
                            print("No report found for the dataset.")
                    if pdf:
                        if report_id is not None:
                            files += [await power_bi_refresh.export_report("PDF")]
                        else:
                            print("No report found for the dataset.")

                    await send_email(
                        recipient_yaml_file_path=recipient_yaml_file_path,
                        subject=subject,
                        body=body,
                        attachments=files
                    )

# Snowflake connection functions
async def sf_pe_prod_connection(
    database: str = 'BUSINESSINTEL01',
    schema: str = 'INFORMATION_SCHEMA',
    role: str = 'DEV_DATA_ENG_FR_AM',
    warehouse: str = 'PROD_INGESTION_DE_WH'
):
    snowflake_credentials_block = await SnowflakeCredentials.load("snowflake-data-engineering-etl")
    username = await snowflake_credentials_block.user.get()
    password = await snowflake_credentials_block.password.get_secret_value()
    account = await snowflake_credentials_block.account.get()
    role = await snowflake_credentials_block.role.get()

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
    return await connection.connect()

async def sf_cpt_connection(
    database: str = 'BUSINESSINTEL01',
    schema: str = 'INFORMATION_SCHEMA',
    role: str = 'ACCOUNTADMIN',
    warehouse: str = 'DEMAND'
):
    env, _ = await load_env_and_secrets()
    if env == 'QA':
        database += '_QA'
    cpt_credentials = await Basic_Credentials.load("sf-old-key")
    username = await cpt_credentials.username.get_secret_value()
    password = await cpt_credentials.password.get_secret_value()
    account_block = await String.load("sf-cpt-account-name")
    account = await account_block.get()

    connection = SnowflakeConnection(
        account=account,
        database=database,
        schema=schema,
        username=username,
        password=password,
        warehouse=warehouse,
        role=role
    )
    return await connection.connect()

async def sf_pe_it_connection(
    database: str = 'IT',
    schema: str = 'INFORMATION_SCHEMA',
    role: str = 'ACCOUNTADMIN',
    warehouse: str = 'IT_ETL'
):
    daie_credentials = await Basic_Credentials.load("sf-daie-key")
    username = await daie_credentials.username.get_secret_value()
    password = await daie_credentials.password.get_secret_value()
    account_block = await String.load("sf-daie-account-name")
    account = await account_block.get()

    connection = SnowflakeConnection(
        account=account,
        database=database,
        schema=schema,
        username=username,
        password=password,
        warehouse=warehouse,
        role=role
    )
    return await connection.connect()










async def process_blob(blob_path: str = None, blob_bytes: bytes = None,parent_folder: str = None, sub_folder: str = None):

    azure_credentials_block = await AzureBlobStorageCredentials.load('azure-blob-storage-credentials-ryobi-01', validate=False)
    blob_client = azure_credentials_block.get_blob_client(container='staging', blob=blob_path)
    try:
        await blob_client.undelete_blob()
    except Exception as e:
        pass

    myblob = await blob_storage_download('staging', blob_path, azure_credentials_block)
    stage_bytes = transform(myblob, blob_path)
    await blob_storage_upload(stage_bytes, 'extstaging', azure_credentials_block, f'raw/{parent_folder}/{sub_folder}/{blob_path}', overwrite=True)

    try:
        await blob_client.delete_blob()
    except Exception as e:
        pass



if __name__ == "__main__":
    asyncio.run(send_email(subject='Test', body='Test', attachments=['recipients.yaml', 'reports.yaml'], message_status='Warning'))
