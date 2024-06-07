import requests  
from prefect.blocks.system import Secret  
import json
from prefect.blocks.system import String

env = String.load("environment").value

def blob_cleanup(blob_name):
    
    secret_block = Secret.load("delete-stage-blob-url")
    delete_blob_url = secret_block.get()

        # Logic App trigger URL
    url = delete_blob_url

    # Create the payload using the user input
    payload = {
        "properties": {
            "blob_name": {
                "type": "string",
                "value": blob_name
            }
        }
    }

    # Send the post request
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, data=json.dumps(payload), headers=headers)
    
    # Print the response
    print(response.status_code, response.reason)
    return response.status_code
