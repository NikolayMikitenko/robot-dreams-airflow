import yaml
import pathlib
import json

class Config():
    def __init__(self, path: pathlib.Path):
        self.path = path
        with open(path, 'r') as f:
            self.config = yaml.safe_load(f)

    def get_app_config(self, app: str):
        config = self.config[app]
        return config

    def validate_app_config(self, app: str, config: json):
        if config['url'] is None:
            raise Exception(f'Config for application "{app}" in file "{str(self.path)} do not contain url.')
        if config.get('endpoint') is None:
            raise Exception(f'Config for application "{app}" in file "{str(self.path)} do not contain endpoint.') 
        if config.get('auth'):        
            if config['auth'].get('endpoint') is None:
                raise Exception(f'Config for application "{app}" in file "{str(self.path)} do not contain endpoint for authentification.')
            if config['auth'].get('parameters') is None:
                raise Exception(f'Config for application "{app}" in file "{str(self.path)} do not contain parameters for authentification.')       
            if config['auth'].get('type') is None:
                raise Exception(f'Config for application "{app}" in file "{str(self.path)} do not contain type for authentification.')
        return True                            
