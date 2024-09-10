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
    with concurrency("email_concurrency", occupy=1):
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

if __name__ == "__main__":
    print(env)
    send_email(subject='Test',body='Test',attachments=['recipients.yaml','reports.yaml'],message_status='Warning')
