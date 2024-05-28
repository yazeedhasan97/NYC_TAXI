import base64
import functools
import json
import logging
import os
import pickle
import subprocess

from cryptography.fernet import Fernet


class RememberMe:
    def __init__(self, path, key=None, logger=None):
        self.path = path
        # self.key = key
        # self.fernet = Fernet(key)
        self.logger = logger if logger else logging.getLogger(__name__)

    def remember_user(self, user):
        try:
            # encrypted_data = self.fernet.encrypt(pickle.dumps(user))
            with open(self.path, 'wb') as file:
                # file.write(encrypted_data)
                file.write(pickle.dumps(user))
            self.logger.info('User data remembered successfully.')
        except Exception as e:
            self.logger.error(f'Error occurred while saving user data: {e}')

    def get_user(self):
        if os.path.exists(self.path):
            try:
                with open(self.path, 'rb') as file:
                    encrypted_data = file.read()
                # decrypted_data = self.fernet.decrypt(encrypted_data)
                decrypted_data = encrypted_data
                user = pickle.loads(decrypted_data)
                self.logger.info('User data retrieved successfully.')
                return user
            except Exception as e:
                self.logger.error(f'Error occurred while retrieving user data: {e}')
        else:
            self.logger.warning('User data file does not exist.')
        return None


def base64_image_converter(img):
    return base64.b64encode(img).decode("utf8")


def timer(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        import time
        start = time.time()
        x = func(*args, **kwargs)
        end = time.time()
        print(f'{func.__name__} Took {end - start} Time to excute')
        return x

    return wrapper


def find_base_directory():
    current_file = os.path.abspath(__file__)
    base_directory = os.path.dirname(current_file)
    return base_directory


def load_json_file(path):
    """Return a dictionary structured exactly [dumped] as the JSON file."""
    try:
        with open(path, encoding='utf-8') as file:
            loaded_dict = json.load(file)
        return loaded_dict
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Error: {e}. File not found at path: {path}")
    except json.JSONDecodeError as e:
        raise Exception(f"Error: {e}. Unable to decode JSON file at path: {path}")
    except Exception as e:
        raise Exception(f"Error: {e}. An unexpected error occurred while loading JSON file at path: {path}")


def run_terminal_command(command):
    try:
        # Run the command and capture the output
        result = subprocess.run(command, shell=True, universal_newlines=True, check=False)
        # print('Terminal Command Results:', result)
        # INFO(f'Terminal Command: {result.args} ')
        # INFO(f'Terminal Command Results: {result.returncode} ')

        return result.returncode
    except subprocess.CalledProcessError as e:
        raise e
    except Exception as e:
        raise e
