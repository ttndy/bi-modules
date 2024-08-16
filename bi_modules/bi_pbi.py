import requests  
import time  
import datetime  
import inspect
from prefect.blocks.system import Secret  
import json
try:
    from .blocks import SystemConfiguration
except:
    from blocks import SystemConfiguration
import yaml
from pathlib import Path
from prefect.blocks.system import String
try:
    from .bi_email import send_email
except:
    from bi_email import send_email
env = String.load("environment").value

system_configuration_block = SystemConfiguration.load("datateam-email-credentials")
system_secrets = system_configuration_block.system_secrets.get_secret_value()

################################################################################################################################
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
  

################################################################################################################################

def report_refresh(
                    number_of_tries: int = 5,
                    send_email_when_done: bool = False,
                    subject: str = '',
                    body: str = '',
                    reports_yaml_file_path: str = find_reports_yaml_path()
                          ) -> None:
    with open(reports_yaml_file_path, 'r') as file:
        reports = yaml.safe_load(file).get('reports', [])
        if env == 'QA':
            return print(f"Running in QA, skipping refresh for the following report(s):<br>{reports}")
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
                                subject=subject,
                                body=body,
                                attachments=files
                                )


def report_refresh_noyaml(
                    number_of_tries: int = 5,
                    send_email_when_done: bool = False,
                    subject: str = '',
                    body: str = '',
                    report_name: str = '',
                    group_name: str = '',
                    email_recipients: str = None,
                          ) -> None:
                          
    if env == 'QA':
        return print(f"Running in QA, skipping refresh for the following report(s):<br>{reports}")
    
    refresh = report.get('Refresh', True)
    export_options = []
    
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
            if email_recipients is not None:
                send_email(
                            subject=subject,
                            body=body,
                            attachments=files,
                            email=email_recipients
                            )
            else:
                send_email(
                            subject=subject,
                            body=body,
                            attachments=files
                            )




if __name__ == "__main__":
    print(env)
