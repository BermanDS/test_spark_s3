import os
import gc
import json
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.parser import parse

from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql.types import *
import pyspark.sql.functions as F


### UDF function ------------------------------------------------
get_hour = F.udf(lambda x: pd.to_datetime(x).hour, IntegerType())


#----------------------------------------------------------------------------
class NpEncoder(json.JSONEncoder):
    """ Custom encoder for numpy data types """
    
    def default(self, obj):
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                            np.int16, np.int32, np.int64, np.uint8,
                            np.uint16, np.uint32, np.uint64)):

            return int(obj)

        elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
            return float(obj)

        elif isinstance(obj, (np.complex_, np.complex64, np.complex128)):
            return {'real': obj.real, 'imag': obj.imag}

        elif isinstance(obj, (np.ndarray,)):
            return obj.tolist()

        elif isinstance(obj, (np.bool_)):
            return bool(obj)

        elif isinstance(obj, (np.void)): 
            return None
        
        elif isinstance(obj, (date, datetime)):
            return obj.isoformat()
        
        return json.JSONEncoder.default(self, obj)

#--------------------------------------- logging -----------------------------
def log(
    logger: object = None, 
    tag: str = 'service', 
    log_level: str = 'info', 
    message: str = '', 
    data: dict = {}
    ) -> None:

    if logger:
        log_info = {
            "level": log_level,
            "time": datetime.now().isoformat(),
            "tag": tag,
            "message": message,
        }
        log_info.update(data)
            
        if log_level == 'info': logger.info(json.dumps(log_info, cls=NpEncoder))
        elif log_level == 'debug':logger.debug(json.dumps(log_info, cls=NpEncoder))
        elif log_level == 'warn': logger.warn(json.dumps(log_info, cls=NpEncoder))
        elif log_level == 'error':logger.error(json.dumps(log_info, cls=NpEncoder))

#------------------------------------------------------------------------------