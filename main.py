from src.logger import logger
from src.impressions import *
from src.config import auth_config, running_config



def init_spark_local(conf: dict = {}) -> object:
    """
    initialize SparkSession in Standalone mode
    """
    
    return (
        SparkSession
        .builder
        .master(f"local[{conf['spark_kernels']}]")
        .appName("S3 environment")
        .config("spark.hadoop.fs.s3a.access.key", conf['aws_access_key_id'])
        .config("spark.hadoop.fs.s3a.secret.key", conf['aws_secret_access_key'])
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0")
        .getOrCreate()
    )


def main(auth_config: dict = {}, running_config: dict = {}) -> None:
    """
    main procedure of implementation current logic
    """
    
    #### define current parameters ---------------------------------------------
    date_run = parse(running_config['date_run'])
    object_key = (
        running_config['path_source_bucket']
        .format(**{
            'year':date_run.year,
            'month':date_run.strftime('%m'),
            'day':date_run.strftime('%d'),
            'date_iso':date_run.date().isoformat()
        })
    )
    log(logger, 's3 configs', 'info', f"Got next path {object_key} in bucket - {auth_config['bucket_name']}")
    auth_config['spark_kernels'] = running_config['spark_kernels']
    
    ### PySpark init -----------------------------------------------------------
    spark = init_spark_local(auth_config)
    log(logger, 'spark init', 'info', f'Got Spark environment with version: {spark.version}.')
    
    ### Read csv from S3 bucket ------------------------------------------------
    spdf = (
        spark
        .read
        .csv(
            f"s3a://{auth_config['bucket_name']}/{object_key}", 
            header=True, 
            inferSchema=True
        )
        .repartition(int(running_config['input_partitions']))
        .cache()
    )
    log(logger, 's3 downloading', 'info', f'Got {spdf.count()} records from {object_key}')
    
    ### Load class for transforming data instance -------------------------------
    go_on = True
    imp = ImpressionsEvents(spdf)
    go_on = imp.main_instance
    
    if not go_on:
        log(logger, 'data processing', 'error', f'Input data :{object_key} : problem with processin raw input')
    else:
        log(logger, 'data processing', 'info', f'Input data :{object_key} processed raw input')
    
    go_on = imp.daily_agg_by_hour()
    if go_on:
        log(logger, 'data processing', 'info', f"Got {imp.data['daily_agg_by_hour'].count()} records for instance: daily_agg_by_hour")
    else:
        log(logger, 'data processing', 'error', f'Problem with processing instance: daily_agg_by_hour')
    
    ### Saving result to S3 bucket -----------------------------------------------
    if go_on:
        object_result = (
            running_config['path_result_bucket']
            .format(**{
                'year':date_run.year,
                'month':date_run.strftime('%m'),
                'day':date_run.strftime('%d'),
                'date_int':date_run.strftime('%Y%m%d'),
                'initials':running_config['initials']
            })
        )
        log(logger, 's3 configs', 'info', f"Got next output path {object_result} in bucket - {auth_config['bucket_name']}")
        #(
        #    imp.data['daily_agg_by_hour']
        #    .coalesce(int(running_config['result_partitions']))
        #    .write
        #    .options(header='True', delimiter=',')
        #    .csv(f"s3a://{auth_config['bucket_name']}/{object_result}")
        #)
    #-----------------------------------------------------------------------------
    
    imp.data.clear()
    del imp.df_main, spdf
    gc.collect()
    spark.stop()

if __name__ == '__main__':
    main(auth_config, running_config)