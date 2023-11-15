#### defining logger instance --------------------------------
import os
import socket
import logging
from importlib import reload
from src.config import root_dir



def logger_init(location: str = '', log_path: str = os.getcwd(), log_level: str = 'info') -> object:

    reload(logging)
    location = location if location != '' else socket.gethostname()

    logging.basicConfig(
            filename = os.path.join(log_path, f'{location}.log'),
            level = logging.INFO if log_level == 'info' \
                    else logging.ERROR if log_level == 'error' \
                    else logging.WARN if log_level == 'warn' \
                    else logging.DEBUG,
    )
    
    return logging.getLogger(__name__)



logger = logger_init('test_task', os.path.join(root_dir, 'log'))