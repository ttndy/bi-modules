import pyodbc
from prefect.blocks.system import Secret, String
from prefect_azure import AzureBlobStorageCredentials, AzureBlobStorageContainer



def connect():
        secret_block_uid = Secret.load("vd-uid")
        secret_block_pwd = Secret.load("vd-pwd")

        uid = secret_block_uid.get()
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

        with open('/opt/' + file , "wb") as f:
            block.download_object_to_file_object(
                from_path='dataconnect/' + file,
                to_file_object=f
            )

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

conn_vd = connect()



if __name__ == '__main__':
        connect()

