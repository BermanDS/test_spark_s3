"""Impressions parser."""
from src.utils import *


class ImpressionsEvents:
    """Class that split an impressions data to separate instances."""

    def __init__(self,
                df_main: object = None
                ):
        """
        :df_main: Main instance of data parsed from CSV file.
        """
        
        self.df_main = df_main
        self.data = {}
        self.required_columns = {
            'main': [
                'IMPRESSION_ID', 'AGENCY_ID', 'ADVERTISER_ID', 'ORDER_ID',
                'CAMPAIGN_ID', 'IMPRESSION_DATETIME', 'IMPRESSION_DATE', 'AD_ID',
                'EXTERNAL_ORDER_ID', 'CREATIVE_MEDIA_TYPE', 'CREATIVE_TYPE',
                'BILLING_TYPE', 'BROWSER_NAME', 'BROWSER_VERSION', 'DEVICE_TYPE',
                'DEVICE_MODEL', 'DEVICE_MAKER', 'OS_NAME', 'IMPRESSION'
            ],
        }
        self.fillna_values = {
            'CAMPAIGN_ID':-1,
        }
        self.maininstances = {
            'daily_agg_by_hour': ['CAMPAIGN_ID', 'IMPRESSION_HOUR', 'IMPRESSION_COUNTS'],
        }
        self.main_instance = False
        self._apply_main_preprocessing()
        
    
    def _apply_main_preprocessing(self) -> None:
        """
        Apply preprocessing the main instance of data with checking of existance all required
        columns as result regarding successful checking - self.main_instance will be True
        """

        if (
            set(self.required_columns['main']) & set(self.df_main.columns) == set(self.required_columns['main']) and 
            not self.df_main.rdd.isEmpty()
            ):
            #--if condition is true -> droping duplicates by set 'IMPRESSION_ID','IMPRESSION_DATETIME' and 
            #--filling Nan values in CAMPAIGN_ID field-------------------------------------------------------
            self.df_main = (
                self.df_main
                .dropDuplicates(['IMPRESSION_ID','IMPRESSION_DATETIME'])
                .na.fill(self.fillna_values)
                .withColumn('CAMPAIGN_ID',F.col('CAMPAIGN_ID').cast(IntegerType()))
                .cache()
            )
            self.main_instance = not self.df_main.rdd.isEmpty()
    

    def daily_agg_by_hour(self) -> bool:
        """
        Assemble instance of daily aggregation by hour and campaign id
        """
        
        self.instance_name = 'daily_agg_by_hour'
        if not self.main_instance:
            return False
        else:
            self.data[self.instance_name] = (
                self.df_main
                .withColumn('IMPRESSION_HOUR', get_hour(F.col('IMPRESSION_DATETIME')))
                .groupBy('CAMPAIGN_ID','IMPRESSION_HOUR')
                .count()
                .withColumnRenamed('count', 'IMPRESSION_COUNTS')
                [self.maininstances[self.instance_name]]
                .cache()
            )
            return not self.data[self.instance_name].rdd.isEmpty()
