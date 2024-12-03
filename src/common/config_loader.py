import os
import yaml
from typing import Dict, Any

class ConfigLoader:
    @staticmethod
    def load_config(config_path: str) -> Dict[str, Any]:
        """Load YAML configuration file"""
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Get current environment
        environment = os.getenv('ENV', config.get('environment', 'development'))
        
        return config[environment]

    @staticmethod
    def get_postgres_config(config_dir: str = None) -> Dict[str, Any]:
        """Load PostgreSQL configuration"""
        config_path = os.path.join(config_dir or '', 'database_config.yml')
        config = ConfigLoader.load_config(config_path)
        return config['postgres']

    @staticmethod
    def get_couchdb_config(config_dir: str = None) -> Dict[str, Any]:
        """Load CouchDB configuration"""
        config_path = os.path.join(config_dir or '', 'database_config.yml')
        config = ConfigLoader.load_config(config_path)
        return config['couchdb']