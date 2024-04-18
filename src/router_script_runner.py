import os
import paramiko

from dotenv import load_dotenv
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)


class RouterScriptRunner:
    def __init__(self):
        self.hostname = os.getenv('HOSTNAME')
        self.port = os.getenv('PORT')
        self.username = os.getenv('USERNAME')
        self.password = os.getenv('PASSWORD')
        self.client = None

    def connect_to_router(self):
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(hostname=self.hostname, port=int(self.port), username=self.username, password=self.password)

    def do_one_command(self):
        stdin, stdout, stderr = self.client.exec_command('ip addr')
        for line in stdout:
            print(line.strip())

    def close_connection(self):
        self.client.close()
