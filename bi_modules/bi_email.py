import inspect
from O365 import Account
from typing import List, Optional
try:
    from .blocks import SystemConfiguration
except:
    from blocks import SystemConfiguration
import yaml
from pathlib import Path
from prefect.blocks.system import String
from prefect.concurrency.sync import concurrency
from prefect.variables import Variable


################################################################################################################################
def find_yaml_path():
    stack = inspect.stack()
    # Skip this function's frame and look for the outermost caller
    # that's not part of the bi_modules package
    for frame in stack[1:]:
        module = inspect.getmodule(frame[0])
        if module is None:
            continue
        
        module_file = module.__file__
        if module_file and Path(module_file).name != '<stdin>':
            module_path = Path(module_file)
            # Check if this module is NOT from bi_modules (i.e., it's a user script)
            if 'bi_modules' not in str(module_path):
                print(f"Found caller path: {module_path.parent / 'recipients.yaml'}")
                return module_path.parent / 'recipients.yaml'
    
    # If we can't determine caller path or caller is from bi_modules, return current working directory
    print(f"Using fallback path: {Path('./recipients.yaml').absolute()}")
    return Path('./recipients.yaml')

def get_recipients_from_yaml():
    """
    Try to load recipients from a yaml file
    Returns list of recipients if file exists and contains valid data, None otherwise
    """
    try:
        yaml_path = find_yaml_path()
        if yaml_path.exists():
            with open(yaml_path, 'r') as f:
                data = yaml.safe_load(f)
                if isinstance(data, dict) and 'recipients' in data:
                    return data['recipients']
    except Exception as e:
        print(f"Error loading recipients from yaml: {e}")
    return None

def login() -> Account:
    system_configuration_block = SystemConfiguration.load("datateam-email-credentials", validate=False)
    system_secrets = system_configuration_block.system_secrets.get_secret_value()

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

    
################################################################################################################################

def send_email(
    subject: str,
    body: str,
    attachments: list = None,
    email: str = None,
    content_ids: dict = None,  # For inline images
    message_status: str = None  # Optional message status
):
    """
    Send an HTML email with optional attachments, inline images, and status-based formatting.
    
    This function is used across various pipeline tools to deliver notifications, reports,
    and alerts. It handles authentication, recipient resolution, and message formatting.
    
    Args:
        subject (str): Email subject line.
        body (str): Email body content in HTML format. Can include HTML tags for formatting.
        attachments (list, optional): List of file paths to attach to the email.
            Example: ['path/to/file1.pdf', 'path/to/file2.xlsx']
        email (str or list, optional): Recipient email address(es). Can be a single email string
            or a list of email addresses. If None, the function will:
            1. Try to load recipients from a nearby 'recipients.yaml' file
            2. Fall back to the default email from "datateam-email" String block
        content_ids (dict, optional): Dictionary mapping attachment file paths to content IDs
            for inline images. Used when embedding images directly in HTML.
            Example: {'/path/to/image.jpg': 'image1'} with HTML: <img src="cid:image1">
        message_status (str, optional): Optional formatting style for the message.
            Accepted values:
            - 'Success': Green border with success header
            - 'Error': Red border with error header
            - 'Warning': Yellow border with warning header
            - None: No special formatting, body sent as-is
    
    Environment Behavior:
        - In QA environment (when Variable.get("env") == 'QA'):
          - All emails are redirected to the default datateam email regardless of 'email' parameter
          - Subject is appended with ' - DEBUG MODE'
        - In other environments, emails are sent to the specified recipients
    
    Concurrency:
        Uses "email_concurrency" to prevent sending too many emails simultaneously
    
    Examples:
        # Simple notification
        send_email(
            subject="Daily Report Completed",
            body="<p>The daily report has been generated successfully.</p>",
            message_status="Success"
        )
        
        # With attachment
        send_email(
            subject="Monthly Report",
            body="<p>Please find the attached report.</p>",
            attachments=["/path/to/report.pdf"]
        )
        
        # With inline image
        send_email(
            subject="Data Visualization",
            body="<p>Here's the chart: <img src='cid:chart1'></p>",
            attachments=["/path/to/chart.png"],
            content_ids={"/path/to/chart.png": "chart1"},
            message_status="Warning"
        )
    """
    with concurrency("email_concurrency", occupy=1):
        account = login()
        
        # Create and send email
        m = account.new_message()
        
        # Handle recipients
        if email is not None:
            recipients = [email] if isinstance(email, str) else email
        else:
            # Try to get recipients from yaml file
            yaml_recipients = get_recipients_from_yaml()
            if yaml_recipients:
                recipients = yaml_recipients
            else:
                recipients = [String.load("datateam-email").value]
                
        env = Variable.get("env")
        if env == 'QA':
            recipients = [String.load("datateam-email").value]
            subject += ' - DEBUG MODE'
            
        m.to.add(recipients)
        m.subject = subject

        # Format body based on message status if provided
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
        m.body_type = 'HTML'

        # Handle attachments with content IDs for inline images
        if attachments:
            for attachment in attachments:
                # If this attachment has a content ID, set it as inline
                if content_ids and attachment in content_ids:
                    m.attachments.add(attachment)
                    # Get the last added attachment
                    last_attachment = m.attachments[-1]
                    # Set it as inline with the content ID
                    last_attachment.is_inline = True
                    last_attachment.content_id = content_ids[attachment]
                else:
                    # Regular attachment
                    m.attachments.add(attachment)
        
        m.send()

if __name__ == "__main__":
    send_email(subject='Test',body='Test',attachments=['recipients.yaml','reports.yaml'],message_status='Warning')
