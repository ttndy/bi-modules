import pyodbc
from prefect.blocks.system import Secret, String
import os
import tempfile

secret_block_uid = Secret.load("vd-uid")
secret_block_pwd = Secret.load("vd-pwd")
secret_block_cacerts = Secret.load("dc-cacerts")

cacerts = secret_block_cacerts.get()
uid = secret_block_uid.get()
pwd = secret_block_pwd.get()
host = String.load("dataconnect-host").value

port="20502"
ssl="1"
dsn="VENDORDRILL"




def connect():
    with tempfile.NamedTemporaryFile(delete=False, suffix='.pem') as temp_cert_file:
        temp_cert_file.write(cacerts.encode())
        temp_cert_path = temp_cert_file.name

    try:
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
                ";TrustedCerts=" + temp_cert_path +
        #                 Your Login Credentials
                ";UID=" + uid +
                ";PWD=" + pwd,autocommit=True)
        return at_scale_connection
    
    except Exception as e:
        print("Connection failed: ", e)
        return None
    
    finally:
        os.unlink(temp_cert_path)


conn_vd = connect()


if __name__ == '__main__':
        connect()
