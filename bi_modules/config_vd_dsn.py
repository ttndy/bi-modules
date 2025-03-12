import pyodbc
from prefect.blocks.system import Secret, String
from prefect_azure import AzureBlobStorageCredentials, AzureBlobStorageContainer
import logging
import os

def conn_vd(bu, division):
    try:
        string_name = f"vd-uid-{bu}-{division}".lower()
        string_block_uid = String.load(string_name)
        secret_block_pwd = Secret.load(f"vd-pwd-shared")

        uid = string_block_uid.value
        pwd = secret_block_pwd.get()

        host=String.load("dataconnect-host").value

        port="20502"
        ssl="1"
        azure_credentials_block = AzureBlobStorageCredentials.load('azure-blob-storage-credentials-ryobi-01',validate=False)
        block = AzureBlobStorageContainer(
            container_name="certs",
            credentials=azure_credentials_block,
        )

        file = "cacerts.pem"
        file_path = os.path.join('/opt/', file)

        # Ensure the directory exists
        os.makedirs('/opt/', exist_ok=True)

        # Check if the file already exists
        if not os.path.isfile(file_path):
            with open(file_path, "wb") as f:
                block.download_object_to_file_object(
                    from_path='dataconnect/' + file,
                    to_file_object=f
                )
            logging.info(f"File {file_path} downloaded successfully.")
        else:
            logging.info(f"File {file_path} already exists, skipping download.")

        at_scale_connection = pyodbc.connect(
        #                 Destination of your HDBC Driver  
                "DRIVER=/opt/cloudera/hiveodbc/lib/64/libclouderahiveodbc64.so" +
                ";HOST=" + host +
                ";PORT=" + port +
                ";HiveServerType=2" +
                ";AuthMech=3" +
                ";SSL=" + ssl +
                ";AllowSelfSignedServerCert=1" +
                ";CAIssuedCertNamesMismatch=1" +
        #                 Destination of your cacerts.pem file
                ";TrustedCerts=" + '/opt/' + file +
        #                 Your Login Credentials
                ";UID=" + uid +
                ";PWD=" + pwd,autocommit=True)
        return at_scale_connection
    except Exception as e:
        logging.error(f"Error in conn_vd: {e}")
        raise

if __name__ == '__main__':
        conn_vd()

