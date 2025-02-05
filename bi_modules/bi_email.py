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

env = String.load("environment").value
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
    Send an email with optional inline images and status formatting
    
    Args:
        subject: Email subject
        body: Email body (HTML)
        attachments: List of file paths to attach
        email: Recipient email address(es)
        content_ids: Dictionary mapping file paths to content IDs for inline images
        message_status: Optional status ('Success', 'Error', 'Warning') for formatting
    """
    with concurrency("email_concurrency", occupy=1):
        account = login()
        
        # Create and send email
        m = account.new_message()
        
        # Handle recipients
        if email is not None:
            recipients = [email] if isinstance(email, str) else email
        else:
            recipients = [String.load("datateam-email").value]
            
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
    print(env)
    send_email(subject='Test',body='Test',attachments=['recipients.yaml','reports.yaml'],message_status='Warning')
