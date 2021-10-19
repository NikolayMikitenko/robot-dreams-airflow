from airflow.operators.python_operator import PythonOperator
from airflow.hooks.http_hook import HttpHook
from pathlib import Path
from datetime import datetime
import config
import json
import requests
import os

class HttpToFileSystemOperator(PythonOperator):
    def __init__(self, config_path: Path, app_name: str, date: datetime.date, timeout: int, file_system_path: Path, *args, **kwargs):
        super(HttpToFileSystemOperator, self).__init__(*args, **kwargs)
        print(os.getcwd())
        print(date)
        self.config_path = config_path
        self.app_name = app_name
        self.date = date
        self.timeout = timeout
        self.file_system_path = file_system_path

    def execute(self, context):
        self.app_config = self.load_config()
        self.validate_app_config()

        token = None
        if self.app_config.get('auth'):
            token = self.get_token()

        self.call_api(token=token)      

    def load_config(self):
        try:
            return config.Config(self.config_path).get_app_config(self.app_name)
        except FileNotFoundError as e:
            e.strerror =  'Config file for app not found. ' + e.strerror
            raise e
        except KeyError as e: 
            raise KeyError(f'Can not find aplication: "{self.app_name}" in config file "{str(self.config_path)}".')

    def validate_app_config(self):
        if not self.app_config.get('url'):
            raise Exception(f'Config for application "{self.app_name}" in file "{str(self.config_path)} do not contain url.')
        if not self.app_config.get('endpoint'):
            raise Exception(f'Config for application "{self.app_name}" in file "{str(self.config_path)} do not contain endpoint.') 
        if self.app_config.get('auth'):        
            if not self.app_config['auth'].get('endpoint'):
                raise Exception(f'Config for application "{self.app_name}" in file "{str(self.config_path)} do not contain endpoint for authentification.')
            if not self.app_config['auth'].get('parameters'):
                raise Exception(f'Config for application "{self.app_name}" in file "{str(self.config_path)} do not contain parameters for authentification.')       
            if not self.app_config['auth'].get('type'):
                raise Exception(f'Config for application "{self.app_name}" in file "{str(self.config_path)} do not contain type for authentification.')     

    def get_token(self):  
        if self.app_config['auth']['type'] == "JWT TOKEN":
            r = requests.post(url=self.app_config['url'] + self.app_config['auth']['endpoint'], headers = self.app_config['headers'], json=self.app_config['auth']['parameters'], timeout=self.timeout)
            if r.status_code == 200:
                if r.json().get('access_token'):
                    return "JWT " + r.json()['access_token']
                else:
                    raise Exception(f"Auth request do not return access_token.")
            else:
                raise Exception(f"Auth request return bad response state_code {str(r.status_code)} with message: {r.text}")
        else:
            return ""                

    def call_api(self, token: str):
        headers = self.app_config['headers']
        if token:
            headers['Authorization'] = token
        
        parameters = {"date": self.date}

        try:
            r = requests.get(url=self.app_config['url'] + self.app_config['endpoint'], headers = headers, json=parameters, timeout=self.timeout)
            if r.status_code == 200:
                if len(r.json()) > 0:
                    self.save_to_file(parameters=parameters, data=r.json())
                else:
                    print(f"[ERROR] Request to url: {self.app_config['url'] + self.app_config['endpoint']} with parameters: {parameters} return empty JSON answer")
            else:
                print(f"[ERROR] Request to url: {self.app_config['url'] + self.app_config['endpoint']} with parameters: {parameters} return bad response state_code {str(r.status_code)} with message: {r.text}")
        except Exception as e:
            print(f"[ERROR] Request to url: {self.app_config['url'] + self.app_config['endpoint']} with parameters: {self.app_config['parameters']} return Exception: {str(e)}")

    def save_to_file(self, parameters: json, data: json):
        os.makedirs(self.file_system_path, exist_ok=True)
        directory_path = os.path.join(self.file_system_path, self.app_name)
        os.makedirs(directory_path, exist_ok=True)
        directory_path = os.path.join(directory_path, parameters[self.app_config['data parameter']])
        os.makedirs(directory_path, exist_ok=True)
        file_path =os.path.join(directory_path, self.app_name + '.json')
        with open(file_path, 'w') as f:
            json.dump(data, f)