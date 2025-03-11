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
import base64
from prefect.variables import Variable


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
        print(f"Dataset request response: {response.status_code}")
        
        if response.status_code == 401:  
            print("Token expired, refreshing...")
            self.api_token, _, _ = self.get_power_bi_access_token()  
            headers = {"Authorization": self.api_token}    
            response = requests.get(url, headers=headers)    
            print(f"Dataset request response after token refresh: {response.status_code}")
        
        if response.status_code != 200:
            print(f"Error getting datasets: {response.status_code}")
            print(f"Response content: {response.text}")
            return None
        
        try:
            datasets = response.json()['value']    
            for dataset in datasets:    
                if dataset['name'] == self.report_name:   
                    return dataset['id']   
        except Exception as e:
            print(f'Error parsing dataset response: {str(e)}')
            print(f'Response content: {response.text}')
        
        print(f'Dataset not found or not configured to be refreshed for {self.report_name}') 
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
                if  report['name'] == self.report_name:   
                    self.dataset_id = report['datasetId'] 
                    print('Report Name: ', report['name'])
                    # Get report pages
                    pages_url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.group_id}/reports/{report['id']}/pages"
                    pages_response = requests.get(pages_url, headers=headers)
                    if pages_response.status_code == 200:
                        pages = pages_response.json()['value']
                        # Store both display name and actual name
                        self.report_pages = [{
                            'displayName': page['displayName'],
                            'name': page['name']
                        } for page in pages]
                        print('Report Pages: ', [page['displayName'] for page in self.report_pages])
                    return report['id'], report['webUrl']
        except json.decoder.JSONDecodeError:  
            print("Failed to decode server response.")  
            return None  
        return None  

    def export_report_to_file(self):    
        self.check_token_refresh()  
        url = f"https://api.powerbi.com/v1.0/myorg/reports/{self.report_id}/ExportTo"    
        headers = {
            "Authorization": self.api_token, 
            "Content-Type": "application/json",
            "Accept": "application/json"
        }    
        
        # Get pages to export
        pages_to_export = getattr(self, 'pages_to_export', None) or [self.report_pages[0]['name']]
        print(f"Exporting pages: {pages_to_export}")
        
        # Configure according to PowerBIReportExportConfiguration schema
        body = {
            "format": self.format_type,
            "powerBIReportConfiguration": {
                "pages": [{"pageName": page_name} for page_name in pages_to_export]
            }
        }

        # Add PDF-specific settings if exporting as PDF
        if self.format_type == "PDF":
            body["powerBIReportConfiguration"].update({
                "settings": {
                    "orientation": "Landscape"
                }
            })
        
        print(f"Sending export request to: {url}")
        print(f"Request body: {json.dumps(body, indent=2)}")
        
        response = requests.post(url, headers=headers, json=body)   
        print(f"Export request response status: {response.status_code}")
        
        if response.status_code != 202:  # Power BI uses 202 for accepted requests
            print(f"Error response content: {response.text}")
            raise ValueError(f"Export request failed with status code: {response.status_code}")
        
        try:
            self.export_id = response.json()['id']
            print(f"Got export ID: {self.export_id}")
            return self.export_id
        except Exception as e:
            print(f"Failed to get export ID from response: {response.text}")
            raise

  
    def check_export_status(self):  
        self.check_token_refresh()  
        url = f"https://api.powerbi.com/v1.0/myorg/reports/{self.report_id}/exports/{self.export_id}"  
        headers = {
            "Authorization": self.api_token,
            "Accept": "application/json"
        }  
        response = requests.get(url, headers=headers)  
        
        if response.status_code not in [200, 202]:  # Both 200 and 202 are valid responses
            print(f"Failed to get status. Status code: {response.status_code}, Content: {response.text}")
            raise ValueError(f"Failed to get export status with status code: {response.status_code}")
        
        status_data = response.json()
        print(f"Export status response: {status_data}")
        
        # Get the actual status from the response
        status = status_data.get('status', '')
        percent_complete = status_data.get('percentComplete', 0)
        print(f"Status: {status}, Progress: {percent_complete}%")
        
        return status
  
    def get_export_file(self):  
        self.check_token_refresh()  
        url = f"https://api.powerbi.com/v1.0/myorg/reports/{self.report_id}/exports/{self.export_id}/file"  
        
        # Set Accept header based on format type
        content_type = "application/pdf" if self.format_type == "PDF" else "image/png"
        headers = {
            "Authorization": self.api_token,
            "Accept": content_type
        }  
        
        print(f"Requesting export file from: {url}")
        response = requests.get(url, headers=headers, stream=True)  
        
        # Debug: Print response headers
        print(f"Response headers: {response.headers}")
        
        if response.status_code != 200:
            print(f"Failed to get export file. Status: {response.status_code}, Content: {response.text}")
            raise ValueError(f"Failed to get export file with status code: {response.status_code}")
        
        content = response.content
        if not content:
            raise ValueError("Received empty content from export file request")
        
        # Debug: Print content type and size
        print(f"Content-Type: {response.headers.get('content-type')}")
        print(f"Content length: {len(content)} bytes")
        
        return content
  
    def export_report(self, format_type="PNG"):  
        self.check_token_refresh()  
        self.format_type = format_type
        
        if not self.report_id:
            raise ValueError(f"No report ID available for report {self.report_name}")
        
        print(f"\nStarting export of report '{self.report_name}' in {format_type} format")
        
        try:
            # Start export  
            self.export_id = self.export_report_to_file()
            
            # Check status  
            print("Waiting for export to complete...")
            status = self.check_export_status()  
            attempts = 0
            max_attempts = 30  # Maximum number of attempts (5 seconds * 30 = 150 seconds timeout)
            
            while status not in ["Succeeded", "Failed"] and attempts < max_attempts:
                time.sleep(5)  # Wait for 5 seconds  
                status = self.check_export_status()  
                attempts += 1
                
            if status == "Failed":
                raise ValueError("Export failed")
            elif status != "Succeeded":
                raise ValueError("Export timed out")

            # Get file  
            print("Export succeeded, downloading file...")
            file_content = self.get_export_file()  
            
            if not file_content:
                raise ValueError("Received empty file content from Power BI")

            # Save file with binary content
            self.report_export_file_name = f"{self.report_name}.{format_type.lower()}"
            print(f"Writing content to file: {self.report_export_file_name}")
            
            # Write the binary content to file
            with open(self.report_export_file_name, "wb") as f:  
                f.write(file_content)  
            
            # Verify file was written correctly
            file_size = Path(self.report_export_file_name).stat().st_size
            print(f"File saved successfully. Size: {file_size} bytes")
            
            if file_size == 0:
                raise ValueError("File was created but is empty")
            
            return self.report_export_file_name
            
        except Exception as e:
            print(f"Error during report export: {str(e)}")
            raise


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
        env = Variable.get("env")
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
                          
    # if env == 'QA':
    #     return print(f"Running in QA, skipping refresh for the following report:<br>{report_name}")
    
    refresh = True
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


def send_report_as_embedded_image(
    report_name: str,
    group_name: str,
    recipients: list,
    subject: str = None,
    body: str = None,
    pages: list = None,  # New parameter for specifying pages
    include_pdf: bool = True,  # New parameter to control PDF export
    number_of_tries: int = 5
) -> None:
    """
    Exports Power BI report pages as PNG for inline display and optionally as PDF for attachment.
    
    Args:
        report_name: Name of the Power BI report
        group_name: Name of the workspace
        recipients: List of email recipients
        subject: Email subject (optional)
        body: Email body text (optional)
        pages: List of page names to export (optional, defaults to first page)
        include_pdf: Whether to include PDF attachment (default True)
        number_of_tries: Number of refresh attempts
    """
    # Initialize PowerBI connection
    power_bi = PowerBiRefresh(report_name, group_name, number_of_tries)
    
    # Get report ID first
    power_bi.report_id, power_bi.report_url = power_bi.get_report_id()
    if power_bi.report_id is None:
        print(f"Error: Could not find report ID for {report_name}")
        return
    
    try:
        # If no pages specified, use the first page
        if not pages:
            pages = [power_bi.report_pages[0]['displayName']]
        
        # Convert display names to internal names if needed
        page_names = []
        for page in pages:
            # Find matching page from report_pages
            matching_page = next(
                (p for p in power_bi.report_pages if p['displayName'].lower() == page.lower()),
                None
            )
            if matching_page:
                page_names.append(matching_page['name'])
            else:
                print(f"Warning: Page '{page}' not found in report")
        
        if not page_names:
            print("No valid pages found to export")
            return
            
        print(f"Exporting pages: {', '.join(pages)}")
        
        # Export PNG for inline display
        power_bi.format_type = "PNG"
        power_bi.pages_to_export = page_names  # Pass the page names to export
        png_file = power_bi.export_report()
        
        # Verify the PNG file exists and has content
        if not Path(png_file).exists() or Path(png_file).stat().st_size == 0:
            print(f"Error: PNG file is empty or does not exist: {png_file}")
            return
        
        # Initialize attachments list and add PNG
        attachments = [png_file]
        
        # Export PDF for attachment if requested
        pdf_file = None
        if include_pdf:
            print("\nExporting PDF version for attachment...")
            pdf_file = power_bi.export_report(format_type="PDF")
            attachments.append(pdf_file)
        
        # Generate default subject if none provided
        if subject is None:
            subject = f"Power BI Report: {report_name}"
        
        # Create a unique Content-ID for the image
        content_id = f"report_image_{int(time.time())}"
        
        # Create the HTML body with the embedded image using CID
        html_content = f"""
        <div style="font-family: Arial, sans-serif; padding: 16px;">
            {f'<div style="margin-bottom: 20px;">{body}</div>' if body else ''}
            <div style="margin-bottom: 20px;">
                <h2 style="color: #4CAF50;">Power BI Report: {report_name}</h2>
                <p>Pages: {', '.join(pages)}</p>
                {f'<a href="{power_bi.report_url}" style="color: #4CAF50; text-decoration: none;">View in Power BI</a>' if power_bi.report_url else ''}
            </div>
            <div>
                <img src="cid:{content_id}" style="max-width: 100%; height: auto;" />
            </div>
        </div>
        """
        
        # Send email with inline PNG and optional PDF attachment
        send_email(
            subject=subject,
            body=html_content,
            email=recipients,
            attachments=attachments,  # List of attachments
            content_ids={png_file: content_id}  # Only PNG is inline
        )
        
        # Clean up the temporary files
        try:
            # Path(png_file).unlink()
            # if pdf_file:
            #     Path(pdf_file).unlink()
            print("Temporary files cleaned up successfully")
        except Exception as e:
            print(f"Warning: Could not delete temporary files: {e}")
        
    except Exception as e:
        print(f"Error during export and email process: {str(e)}")
        raise

if __name__ == "__main__":

    send_report_as_embedded_image(
    report_name="",
    group_name="",
    recipients=["test@test.com"],
    pages=["Net $ to Budget Tracking"],  # Specify pages by their display names
    subject="",
    body="Here's the latest report data:",
    include_pdf=False
    )