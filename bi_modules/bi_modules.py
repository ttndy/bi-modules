from click import group
import requests  
import time  
import datetime  
import inspect
from prefect.blocks.system import Secret  
import json
from O365 import Account
from typing import List, Optional
try:
    from .blocks import SystemConfiguration,Basic_Credentials
except:
    from blocks import SystemConfiguration,Basic_Credentials

import yaml
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
import snowflake.connector
from prefect.blocks.system import JSON
from prefect.blocks.system import String

env = String.load("environment").value
from prefect_snowflake import SnowflakeCredentials

system_configuration_block = SystemConfiguration.load("datateam-email-credentials")
system_secrets = system_configuration_block.system_secrets.get_secret_value()



################################################################################################################################
def find_yaml_path():
    stack = inspect.stack()
    # Start from 1 to skip the current function's frame
    return 'recipients.yaml'
    for frame in stack[1:]:
        module = inspect.getmodule(frame[0])
        if module is None:
            continue
        module_file = module.__file__
        if module_file and Path(module_file).name != '<stdin>':
            return Path(module_file).parent / 'recipients.yaml'
    raise FileNotFoundError("Cannot determine the caller's path for recipients.yaml")

def find_reports_yaml_path():
    stack = inspect.stack()
    # Start from 1 to skip the current function's frame
    return 'reports.yaml'
    for frame in stack[1:]:
        module = inspect.getmodule(frame[0])
        if module is None:
            continue
        module_file = module.__file__
        if module_file and Path(module_file).name != '<stdin>':
            return Path(module_file).parent / 'reports.yaml'
    raise FileNotFoundError("Cannot determine the caller's path for reports.yaml")

def login() -> Account:
    account = Account(
        credentials=(system_secrets['client_id'], system_secrets['client_secret']),
        auth_flow_type='credentials',
        tenant_id=system_secrets['tenant_id'],
        main_resource= String.load("datateam-email").value
    )
    
    assert account.authenticate(), 'Authentication Error'
    print('Authenticated')
    return account


def read_inbox() -> None:
    account = login()
    mail = account.mailbox()
    inbox = mail.inbox_folder()
    for message in inbox.get_messages():
        print(message)




class PowerBiRefresh:    
    def __init__(self, report_name: str, group_name: str, number_of_tries: int = 5):    
        self.report_name = report_name    
        self.group_name = group_name
        self.number_of_tries = number_of_tries
        self.api_token, self.start_time, self.expires_in = self.get_power_bi_access_token()  
        self.group_id = self.get_group_id()
        self.dataset_id = self.get_dataset_id()
        self.report_id,self.report_url = self.get_report_id()


    def get_power_bi_access_token(self):      
        pbi_api_uid_block = Secret.load("pbi-api-uid")    
        pbi_api_pwd_block = Secret.load("pbi-api-pwd")    
        pbi_api_cid_block = Secret.load("pbi-api-cid")    
        pbi_api_cse_block = Secret.load("pbi-api-cse")    
        pbi_api_uid = pbi_api_uid_block.get()    
        pbi_api_pwd = pbi_api_pwd_block.get()    
        pbi_api_cid = pbi_api_cid_block.get()    
        pbi_api_cse = pbi_api_cse_block.get()    
        increment = 0       
        while increment <= 5:      
            try:      
                url = "https://login.microsoftonline.com/common/oauth2/token"      
                payload = {      
                    "username": pbi_api_uid,      
                    "password": pbi_api_pwd,      
                    "client_id": pbi_api_cid,      
                    "client_secret": pbi_api_cse,      
                    "resource": "https://analysis.windows.net/powerbi/api",      
                    "grant_type": "password",      
                }      
                headers = {"Content-Type": "application/x-www-form-urlencoded"}      
                response = requests.request("POST", url, headers=headers, data=payload)      
                api_token = "Bearer " + response.json()["access_token"]        
                expires_in = int(response.json()['expires_in'])  # convert 'expires_in' to integer  
                break      
            except Exception as e:      
                print('Error: \n' + str(e))      
                time.sleep(60)      
                increment = increment + 1      
        return api_token, datetime.datetime.now(), expires_in  
  
    def check_token_refresh(self):  
        elapsed_time = datetime.datetime.now() - self.start_time  
        if elapsed_time.seconds > self.expires_in - 60:  # refresh 60 seconds before token gets expired  
            self.api_token, self.start_time, self.expires_in = self.get_power_bi_access_token()  
  
    def get_group_id(self):  
        self.check_token_refresh() 
        api_token, datetime, expires_in = self.get_power_bi_access_token()  
        url = "https://api.powerbi.com/v1.0/myorg/groups"    
        headers = {"Authorization": api_token} 
        response = requests.get(url, headers=headers)    
        if response.status_code == 401:  
            self.api_token, _, _ = self.get_power_bi_access_token()  
            headers = {"Authorization": self.api_token}    
            response = requests.get(url, headers=headers)    
        print(response)
        groups = response.json()['value']    
        for group in groups:
            if group['name'].lower() == self.group_name.lower():    
                return group['id']
        return None

    def get_dataset_id(self):  
        self.check_token_refresh()  
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.group_id}/datasets"    
        headers = {"Authorization": self.api_token}    
        response = requests.get(url, headers=headers)    
        if response.status_code == 401:  
            self.api_token, _, _ = self.get_power_bi_access_token()  
            headers = {"Authorization": self.api_token}    
            response = requests.get(url, headers=headers)    
        datasets = response.json()['value']    
        try:
            for dataset in datasets:    
                if dataset['name'] == self.report_name:   
                    return dataset['id']   
        except:
            print(f'Error: Dataset not found or not configured to be refreshed for {self.report_name}') 
        return None    
  
    def refresh_power_bi_dataset(self):    
        self.check_token_refresh()  
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.group_id}/datasets/{self.dataset_id}/refreshes"    
        headers = {"Authorization": self.api_token}    
        i = 0    
        while i < 5:  
            try:  
                response = requests.post(url, headers=headers)  
                if response.status_code == 401:  
                    self.api_token, _, _ = self.get_power_bi_access_token()  
                    headers = {"Authorization": self.api_token}    
                    response = requests.post(url, headers=headers)  
                time.sleep(10)  
                return response  
            except Exception as e:  
                print('Power BI API refresh error on attempt ' + str(i) + ' /n' + "Error Description: " + str(e))  
                i = i + 1  
                time.sleep(10)  
  
    def get_dataset_source(self):    
        self.check_token_refresh()  
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.group_id}/datasets/{self.dataset_id}/datasources"   
        headers = {"Authorization": self.api_token}    
        i = 0    
        while i < 5:  
            try:  
                response = requests.post(url, headers=headers)  
                if response.status_code == 401:  
                    self.api_token, _, _ = self.get_power_bi_access_token()  
                    headers = {"Authorization": self.api_token}    
                    response = requests.post(url, headers=headers)  
                time.sleep(10)  
                return response  
            except Exception as e:  
                print('Power BI API get dataset source error on attempt ' + str(i) + ' /n' + "Error Description: " + str(e))  
                i = i + 1  
                time.sleep(10)  

    def power_bi_dataset_refresh_status(self):  
        self.check_token_refresh()
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.group_id}/datasets/{self.dataset_id}/refreshes?$top=1"  
        headers = {"Authorization": self.api_token}  
        response_status = requests.get(url, headers=headers)  
        if response_status.status_code == 401:  
            return None
        json_data = response_status.json()
        return json_data['value'][0]['status']

    def power_bi_check_refresh_status(self):
        self.check_token_refresh()
        i = 0
        while i < self.number_of_tries:
            status = self.power_bi_dataset_refresh_status()
            if status is None:
                self.api_token, _, _ = self.get_power_bi_access_token()
                status = self.power_bi_dataset_refresh_status()
            if status == 'Completed':
                print('Status Check: Completed')
                if self.report_id is not None:
                    print('Report ID: ', self.report_id)
                else:
                    print('No report found for the dataset.')
                return status
            else:
                print(f"""
                            Status Check:
                      Not completed, checking again in {i+1} minute(s)
                      """)
                time.sleep((i+1)*60)
                i = i + 1
        elapsed_time = datetime.datetime.now() - self.start_time
        if elapsed_time.seconds > 3600:
            self.api_token, _, _ = self.get_power_bi_access_token()
        return status


    def trigger_and_check_refresh(self):
        self.check_token_refresh()
        # Trigger refresh      
        refresh_response = self.refresh_power_bi_dataset()        
        if refresh_response.status_code == 202:    
            print("Refresh Response: Success, refresh is in progress.")   
        elif refresh_response.status_code == 400:
            print(f"Refresh Response: A refresh for dataset belonging to {self.report_name} is already in progress. ") 
        else:    
            print("Refresh Response: Failed, received status code: ", refresh_response.status_code)    
    
        # Check refresh status      
        status_check_response = self.power_bi_check_refresh_status()      
        print("Status Check Response: ", status_check_response)    
    
        return refresh_response, status_check_response  
    


    def get_report_id(self):   
        self.check_token_refresh() 
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.group_id}/reports"    
        headers = {"Authorization": self.api_token}    
        response = requests.get(url, headers=headers)    
        print(f"get report id returned... {response}")
        if response.status_code == 401:  
            self.api_token, _, _ = self.get_power_bi_access_token()  
            headers = {"Authorization": self.api_token}    
            response = requests.get(url, headers=headers)    
        if response.status_code != 200:    
            print(f"Error: Received status code {response.status_code} from server.")  
            return None  
        if not response.text:  
            print("No reports available for this dataset.")  
            return None  
        try:  
            reports = response.json()['value']  
            for report in reports:    
                if report['datasetId'] == self.dataset_id:    
                    print('Report Name: ', report['name'])  
                    return report['id'], report['webUrl']
        except json.decoder.JSONDecodeError:  
            print("Failed to decode server response.")  
            return None  
        return None  

    def export_report_to_file(self):    
        self.check_token_refresh()  
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.group_id()}/reports/{self.report_id}/ExportTo"    
        headers = {"Authorization": self.api_token, "Content-Type": "application/json"}    
        body = {"format": self.format_type}    
        response = requests.post(url, headers=headers, json=body)   
        self.export_id = response.json()['id']
        return self.export_id

  
    def check_export_status(self):  
        self.check_token_refresh()  
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.group_id}/reports/{self.report_id}/exports/{self.export_id}"  
        headers = {"Authorization": self.api_token}  
        response = requests.get(url, headers=headers)  
        self.export_status = response.json()['status']
        return self.export_status
  
    def get_export_file(self):  
        self.check_token_refresh()  
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.group_id}/reports/{self.report_id}/exports/{self.export_id}/file"  
        headers = {"Authorization": self.api_token}  
        response = requests.get(url, headers=headers)  
        self.export_file = response.content 
        return self.export_file 
  
    def export_report(self,format_type = "PPTX"):  
        self.check_token_refresh()  
        # Start export  
        self.export_report_to_file()
        # Check status  
        status = self.check_export_status()  
        while status != "Succeeded":  
            time.sleep(5)  # Wait for 5 seconds  
            status = self.check_export_status()  

        # Get file  
        file_content = self.get_export_file()  

        self.report_export_file_name = f"{self.report_name}.{format_type.lower()}"
        # Save file  
        with open(self.report_export_file_name, "wb") as f:  
            f.write(file_content)  
  
        print("Report exported successfully.")  
        return self.report_export_file_name


    def pbi_refresh(self):  
        self.check_token_refresh()  
        # Trigger refresh  
        refresh_response = self.refresh_power_bi_dataset()  
        if refresh_response.status_code == 202:  
            print("Refresh Response: Success, refresh is in progress.")  
        else:  
            print("Refresh Response: Failed, received status code: ", refresh_response.status_code)  
  
        # Check refresh status  
        status_check_response = self.power_bi_check_refresh_status()  
        print("Status Check Response: ", status_check_response)  
  
        # Export report  
        # report_id,webUrl = self.get_report_id( self.get_dataset_id() )  
        # if report_id is not None:  
        #     self.export_report(report_id)  
        # else:  
        #     print("No report found for the dataset.")
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

def send_email(recipient_yaml_file_path: str = find_yaml_path(), 
               subject: str = 'test', 
               body: str = 'test', 
               attachments: List[str] = [],
               message_status: Optional[str] = None, 
               email: str = None) -> None:
    
    """
    Sends an email to a specified recipient.

    :param recipient_yaml_file_path: Path to the YAML file containing recipient information.
    :param subject: Subject line of the email.
    :param body: Body of the email.
    :param attachments: A list of file paths to attach to the email.
    :param message_status: Status of the message, allowable values are "Success", "Error", "Warning".
    :param email: Email address of the sender.
    """
    # List of allowable message statuses
    allowable_statuses = ["Success", "Error", "Warning"]
    if message_status is not None and message_status not in allowable_statuses:
        raise ValueError(f"message_status must be one of {allowable_statuses}, got '{message_status}' instead.")
    print(f"Message Status: {message_status}")  # Proceed with the function


    account = login()
    # Read list of recipient emails from YAML file
    with open(recipient_yaml_file_path, 'r') as file:
        load_yaml = yaml.safe_load(file)
        recipients = load_yaml['recipients']
        if message_status == "Error":
            try:
                print(load_yaml['error_recipients'])
                recipients += load_yaml['error_recipients']
            except:
                print("No error_recipients found in the yaml file, please update")
        if email is not None:
            recipients += [email]
    # Create and send email
    m = account.new_message()
    if env == 'QA':
        recipients = [String.load("datateam-email").value]
        if message_status == 'Error':
            try:
                print(load_yaml['error_recipients'])
            except:
                print("No error_recipients found in the yaml file, please update")
        if email is not None:
            recipients += [email]
            
        subject += ' - DEBUG MODE' 
    m.to.add(recipients)
    m.subject = subject
    if message_status == 'Success':
        body_formatted = f"""
            <div style="font-family: Arial, sans-serif; border: 2px solid #4CAF50; padding: 16px; border-radius: 8px; background-color: #DFF2BF;">
                <h2 style="color: #4CAF50;">Success:</h2>
                <h3 style="color: #4CAF50;">Log Contents:</h3>
                <div style="overflow-x: auto;">{body}</div>
                <br>
            </div>
            """
    elif message_status == 'Error':
        body_formatted = f"""
        <div style="font-family: Arial, sans-serif; border: 2px solid #FF0000; padding: 16px; border-radius: 8px; background-color: #FFCFCF;">
            <h2 style="color: #FF0000;">Error Log Contents:</h2>
            <h4 style="overflow-x: auto;">{body}</h4>
        </div>
        """
    elif message_status == 'Warning':
        body_formatted = f"""
            <div style="font-family: Arial, sans-serif; border: 2px solid #FFC107; padding: 16px; border-radius: 8px; background-color: #FFF9C4;">
                <h2 style="color: #FFC107;">Warning:</h2>
                <p style="font-size: 18px;">{body}</p>
            </div>
        """
    else:
        body_formatted = body
        
    m.body = body_formatted

    for attachment in attachments:
        m.attachments.add(attachment)
    m.send()


def report_refresh(
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
                power_bi_refresh = PowerBiRefresh(report_name, group_name,number_of_tries)  
                power_bi_refresh.pbi_refresh()


                if send_email_when_done:  
                    report_id,webUrl = power_bi_refresh.get_report_id()

                    html_content = f'''
                    <div style="font-family: Arial, sans-serif; border: 2px solid #4CAF50; padding: 16px; border-radius: 8px; background-color: #f9f9f9;">
                        <h2 style="color: #4CAF50;">Power BI Refresh: <span style="font-weight: bold;">{report_name}</span> completed successfully.</h2>
                        <p style="font-size: 18px;">Click the button below to view the report:</p>
                        <br><a href="{webUrl}" style="background-color: #4CAF50; color: white; padding: 14px 20px; margin: 8px 0; border: none; cursor: pointer; border-radius: 4px; text-decoration: none;">Link to Report</a><br>
                    </div>
                    '''

                    if body == '':
                        body = html_content
                    if subject == '':
                        subject = f"Power BI Refresh: {report_name}"
                
                    files = []

                    if pptx:
                        if report_id is not None:
                            files += [power_bi_refresh.export_report(report_name,report_id,"PPTX")]
                        else:
                            print("No report found for the dataset.")
                    if png:
                        if report_id is not None:
                            files += [power_bi_refresh.export_report(report_name,report_id,"PNG")]
                        else:
                            print("No report found for the dataset.")
                    if pdf:
                        if report_id is not None:
                            files += [power_bi_refresh.export_report(report_name,report_id,"PDF")]
                        else:
                            print("No report found for the dataset.")

                    send_email(
                                recipient_yaml_file_path=recipient_yaml_file_path,
                                subject=subject,
                                body=body,
                                attachments=files
                                )



# def report_export(
#                     report_names: list,
#                     number_of_tries: int = 5,
#                     subject: str = '',
#                     body: str = '',
#                     recipient_yaml_file_path: str = find_yaml_path(),
#                       pdf: bool = False,
#                         png: bool = False,
#                           pptx: bool = False
#                           ) -> None:

#     files = []
#     html_content = ''

#     for report_name in report_names:
#         power_bi_refresh = PowerBiRefresh(report_name)  
#         report_id,webUrl = power_bi_refresh.get_report_id(power_bi_refresh.get_dataset_id())

#         html_content += f'''
#         <div style="font-family: Arial, sans-serif; border: 2px solid #4CAF50; padding: 16px; border-radius: 8px; background-color: #f9f9f9;">
#             <h2 style="color: #4CAF50;">Power BI Export: <span style="font-weight: bold;">{report_name}</span> completed successfully.</h2>
#             <p style="font-size: 18px;">Click the button below to view the report:</p>
#             <br><a href="{webUrl}" style="background-color: #4CAF50; color: white; padding: 14px 20px; margin: 8px 0; border: none; cursor: pointer; border-radius: 4px; text-decoration: none;">Link to Report</a><br>
#         </div>
#         '''


#         if pptx:
#             if report_id is not None:
#                 files += [power_bi_refresh.export_report(report_name,report_id,"PPTX")]
#             else:
#                 print("No report found for the dataset.")
#         if png:
#             if report_id is not None:
#                 files += [power_bi_refresh.export_report(report_name,report_id,"PNG")]
#             else:
#                 print("No report found for the dataset.")
#         if pdf:
#             if report_id is not None:
#                 files += [power_bi_refresh.export_report(report_name,report_id,"PDF")]
#             else:
#                 print("No report found for the dataset.")

#     return files,html_content









def sf_pe_prod_connection(database: str = 'BUSINESSINTEL01'
                          ,schema: str = 'INFORMATION_SCHEMA'
                          ,role: str = 'DEV_DATA_ENG_FR_AM'
                          ,warehouse: str = 'PROD_INGESTION_DE_WH'
                          ):
    
    snowflake_credentials_block = SnowflakeCredentials.load("snowflake-data-engineering-etl")
    username = snowflake_credentials_block.user
    password = snowflake_credentials_block.password.get_secret_value()
    account = snowflake_credentials_block.account
    role = snowflake_credentials_block.role


    connection = SnowflakeConnection(
                    account=account,
                    database=database,
                    schema=schema,
                    username=username,
                    password=password,
                    warehouse=warehouse,
                    role=role
                )

    return connection.connect()

def sf_cpt_connection(database: str = 'BUSINESSINTEL01'
                      ,schema: str = 'INFORMATION_SCHEMA'
                      ,role: str = 'ACCOUNTADMIN'
                      ,warehouse: str = 'DEMAND'
                      ):
    
    if env == 'QA':
        database += '_QA'
    cpt_credentials = Basic_Credentials.load("sf-old-key")
    username = cpt_credentials.username.get_secret_value()
    password = cpt_credentials.password.get_secret_value()
    connection = SnowflakeConnection(
                    account=String.load("sf-cpt-account-name").value,
                    database=database,
                    schema=schema,
                    username=username,
                    password=password,
                    warehouse=warehouse,
                    role=role
                )
    
    return connection.connect()


def sf_pe_it_connection(database: str = 'IT'
                        ,schema: str = 'INFORMATION_SCHEMA'
                        ,role: str = 'ACCOUNTADMIN'
                        ,warehouse: str = 'IT_ETL'
                        ):
    daie_credentials = Basic_Credentials.load("sf-daie-key")
    username = daie_credentials.username.get_secret_value()
    password = daie_credentials.password.get_secret_value()
    connection = SnowflakeConnection(
                    account=String.load("sf-daie-account-name").value,
                    database=database,
                    schema=schema,
                    username=username,
                    password=password,
                    warehouse=warehouse,
                    role=role
                )
    return connection.connect()





if __name__ == "__main__":
    print(env)
    # report_refresh(send_email_when_done=True)
    send_email(subject='Test',body='Test',attachments=['recipients.yaml','reports.yaml'],message_status='Warning')
    # refresh_report("LOGILITY - CPT PSI", send_email_when_done=True,pptx=True)
