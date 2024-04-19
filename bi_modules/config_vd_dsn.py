import pyodbc
from prefect.blocks.system import Secret, String
secret_block_uid = Secret.load("vd-uid")
secret_block_pwd = Secret.load("vd-pwd")

uid = secret_block_uid.get()
pwd = secret_block_pwd.get()
port="20502"
host=String.load("dataconnect-host").value
ssl="1"
dsn="VENDORDRILL"
# pyodbc.autocommit = True

def connect():
    pyodbc.autocommit = True
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
        ";TrustedCerts=/opt/cacerts.pem" +
#                 Your Login Credentials
        ";UID=" + uid +
        ";PWD=" + pwd,AutoCommit=True)
    return at_scale_connection

conn_vd = connect()
# conn_vd = 'test 123'

if __name__ == '__main__':
        connect()
        # print(conn_vd)