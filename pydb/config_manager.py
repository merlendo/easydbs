import configparser
from pathlib import Path

DEFAULT_CONFIG_PATH = Path(__file__).parent / 'config.ini'


class ConfigManager:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config['DEFAULT'] = {'DB': 'sqllite', 'DB_NAME': 'app.db'}
        self.path_user_config = Path.cwd() / 'pydb.ini'
        if self.path_user_config.exists():
            self.config.read(self.path_user_config)
        else:
            self._write(DEFAULT_CONFIG_PATH)

    def _write(self, name_file):
        with open(name_file, 'w') as configfile:
            self.config.write(configfile)
