import pytest
import sys
sys.path.append('../')
import numpy as np
from src.impressions import *


@pytest.fixture
def main_instance():
    return [
        {'IMPRESSION_ID': '7a721b91-e3e4-4c30-b965-934c36bee941','AGENCY_ID': 6433,'ADVERTISER_ID': 6086,'ORDER_ID': 7369,
         'CAMPAIGN_ID': 9624,'IMPRESSION_DATETIME': '2021-01-30 12:15:02.000','IMPRESSION_DATE': '2021-01-30','AD_ID': 16861,
         'EXTERNAL_ORDER_ID': 8297183,'CREATIVE_MEDIA_TYPE': 'audio','CREATIVE_TYPE': 'audio_inline','BILLING_TYPE': 'sold',
         'BROWSER_NAME': np.nan,'BROWSER_VERSION': np.nan,'DEVICE_TYPE': 'Speaker','DEVICE_MODEL': 'Amazon_Alexa',
         'DEVICE_MAKER': 'Amazon','OS_NAME': np.nan,'IMPRESSION': 1},
        {'IMPRESSION_ID': '4b63c372-9b5e-40fa-bec0-b863d48e6e8e','AGENCY_ID': np.nan,'ADVERTISER_ID': np.nan,'ORDER_ID': np.nan,
         'CAMPAIGN_ID': np.nan,'IMPRESSION_DATETIME': '2021-01-30 12:40:05.000','IMPRESSION_DATE': '2021-01-30','AD_ID': np.nan,
         'EXTERNAL_ORDER_ID': np.nan,'CREATIVE_MEDIA_TYPE': np.nan,'CREATIVE_TYPE': np.nan,'BILLING_TYPE': np.nan,
         'BROWSER_NAME': np.nan,'BROWSER_VERSION': np.nan, 'DEVICE_TYPE': 'Speaker', 'DEVICE_MODEL': np.nan,
         'DEVICE_MAKER': np.nan,'OS_NAME': 'Windows','IMPRESSION': 1},
        {'IMPRESSION_ID': '759de035-5419-4d61-9218-704b6874e407','AGENCY_ID': 6440,'ADVERTISER_ID': 6125,'ORDER_ID': 6104,
         'CAMPAIGN_ID': 9236,'IMPRESSION_DATETIME': '2021-01-30 19:41:06.000','IMPRESSION_DATE': '2021-01-30','AD_ID': 15668,
         'EXTERNAL_ORDER_ID': np.nan,'CREATIVE_MEDIA_TYPE': 'audio','CREATIVE_TYPE': 'audio_inline','BILLING_TYPE': 'filler',
         'BROWSER_NAME': 'Chrome','BROWSER_VERSION': 'Chrome_84','DEVICE_TYPE': 'Speaker','DEVICE_MODEL': 'Chrome_Cast',
         'DEVICE_MAKER': 'Google','OS_NAME': 'Linux','IMPRESSION': 1},
        {'IMPRESSION_ID': '95cf0fa9-4870-4b2d-b0a0-cbc9f9694a22','AGENCY_ID': 6470,'ADVERTISER_ID': 6698,'ORDER_ID': 7358,
         'CAMPAIGN_ID': 9591,'IMPRESSION_DATETIME': '2021-01-30 12:30:06.000','IMPRESSION_DATE': '2021-01-30','AD_ID': 16699,
         'EXTERNAL_ORDER_ID': 8297188,'CREATIVE_MEDIA_TYPE': 'audio','CREATIVE_TYPE': 'audio_inline','BILLING_TYPE': 'sold',
         'BROWSER_NAME': np.nan,'BROWSER_VERSION': np.nan,'DEVICE_TYPE': 'Speaker','DEVICE_MODEL': 'Amazon_Alexa',
         'DEVICE_MAKER': 'Amazon','OS_NAME': np.nan,'IMPRESSION': 1},
        {'IMPRESSION_ID': '80c406c4-edb2-42bd-a0c3-6f1ece10838e','AGENCY_ID': 6435,'ADVERTISER_ID': 6087,'ORDER_ID': 7278,
         'CAMPAIGN_ID': 9359,'IMPRESSION_DATETIME': '2021-01-30 14:58:15.000','IMPRESSION_DATE': '2021-01-30','AD_ID': 16021,
         'EXTERNAL_ORDER_ID': 8295538,'CREATIVE_MEDIA_TYPE': 'audio','CREATIVE_TYPE': 'audio_inline','BILLING_TYPE': 'sold',
         'BROWSER_NAME': np.nan,'BROWSER_VERSION': np.nan,'DEVICE_TYPE': np.nan,'DEVICE_MODEL': np.nan,
         'DEVICE_MAKER': np.nan,'OS_NAME': 'Windows','IMPRESSION': 1},
        {'IMPRESSION_ID': '1f05fd22-9190-4853-a51f-36d8b08591ed','AGENCY_ID': 6435,'ADVERTISER_ID': 6687,'ORDER_ID': 7323,
         'CAMPAIGN_ID': 9530,'IMPRESSION_DATETIME': '2021-01-30 20:28:26.000','IMPRESSION_DATE': '2021-01-30','AD_ID': 16404,
         'EXTERNAL_ORDER_ID': 8296730,'CREATIVE_MEDIA_TYPE': 'audio','CREATIVE_TYPE': 'audio_inline','BILLING_TYPE': 'sold',
         'BROWSER_NAME': np.nan,'BROWSER_VERSION': np.nan,'DEVICE_TYPE': 'Phone','DEVICE_MODEL': np.nan,
         'DEVICE_MAKER': np.nan,'OS_NAME': 'iOS','IMPRESSION': 1},
        {'IMPRESSION_ID': '4fbcfdc2-2377-4a07-aa34-197fce329b91','AGENCY_ID': np.nan,'ADVERTISER_ID': np.nan,'ORDER_ID': np.nan,
         'CAMPAIGN_ID': np.nan,'IMPRESSION_DATETIME': '2021-01-30 13:34:18.000','IMPRESSION_DATE': '2021-01-30','AD_ID': np.nan,
         'EXTERNAL_ORDER_ID': np.nan,'CREATIVE_MEDIA_TYPE': np.nan,'CREATIVE_TYPE': np.nan,'BILLING_TYPE': np.nan,
         'BROWSER_NAME': np.nan,'BROWSER_VERSION': np.nan, 'DEVICE_TYPE': 'Speaker', 'DEVICE_MODEL': 'Amazon_Alexa',
         'DEVICE_MAKER': 'Amazon','OS_NAME': np.nan,'IMPRESSION': 1},
        {'IMPRESSION_ID': '740d66e8-77b5-4417-80d2-35219bc86b0c','AGENCY_ID': 6440,'ADVERTISER_ID': 6125,'ORDER_ID': 6122,
         'CAMPAIGN_ID': 9261,'IMPRESSION_DATETIME': '2021-01-30 15:49:44.000','IMPRESSION_DATE': '2021-01-30','AD_ID': 15745,
         'EXTERNAL_ORDER_ID': np.nan,'CREATIVE_MEDIA_TYPE': 'audio','CREATIVE_TYPE': 'audio_inline','BILLING_TYPE': 'filler',
         'BROWSER_NAME': np.nan,'BROWSER_VERSION': np.nan,'DEVICE_TYPE': 'Speaker','DEVICE_MODEL': 'Amazon_Alexa',
         'DEVICE_MAKER': 'Amazon','OS_NAME': np.nan,'IMPRESSION': 1},
        {'IMPRESSION_ID': '8dbaa789-06be-4c20-bdc0-66373b47f259','AGENCY_ID': np.nan,'ADVERTISER_ID': np.nan,'ORDER_ID': np.nan,
         'CAMPAIGN_ID': np.nan,'IMPRESSION_DATETIME': '2021-01-30 16:31:49.000','IMPRESSION_DATE': '2021-01-30','AD_ID': np.nan,
         'EXTERNAL_ORDER_ID': np.nan,'CREATIVE_MEDIA_TYPE': np.nan,'CREATIVE_TYPE': np.nan,'BILLING_TYPE': np.nan,
         'BROWSER_NAME': np.nan,'BROWSER_VERSION': np.nan,'DEVICE_TYPE': 'Speaker','DEVICE_MODEL': 'Amazon_Alexa',
         'DEVICE_MAKER': 'Amazon','OS_NAME': np.nan,'IMPRESSION': 1}
    ]


@pytest.fixture
def need_columns():
    return [
        'IMPRESSION_ID', 'AGENCY_ID', 'ADVERTISER_ID', 'ORDER_ID', 'CAMPAIGN_ID', 'IMPRESSION_DATETIME', 
    ]


def init_spark():
    
    return (
        SparkSession
        .builder
        .master("local[1]")
        .appName("test-app")
        .getOrCreate()
    )


def test__apply_main_preprocessing(main_instance, need_columns):
    
    spark = init_spark()
    imp = ImpressionsEvents(
        spark
        .createDataFrame(
            [
                tuple(
                    None if isinstance(x, (float, int)) and np.isnan(x) else x
                    for x in record.tolist()
                )
                for record in pd.DataFrame(main_instance).to_records(index=False)
            ], 
            list(main_instance[0].keys())
        )
    )

    assert imp.main_instance is True
    assert imp.data == {}
    assert imp.df_main.count() > 0
    assert set(imp.df_main.columns) & set(need_columns) == set(need_columns)
    

def test_daily_agg_by_hour(main_instance):
    
    spark = init_spark()
    imp = ImpressionsEvents(
        spark
        .createDataFrame(
            [
                tuple(
                    None if isinstance(x, (float, int)) and np.isnan(x) else x
                    for x in record.tolist()
                )
                for record in pd.DataFrame(main_instance).to_records(index=False)
            ], 
            list(main_instance[0].keys())
        )
    )
    load = imp.daily_agg_by_hour()

    assert all([imp.main_instance, load]) is True
    assert 'daily_agg_by_hour' in imp.data
    assert imp.data['daily_agg_by_hour'].count() > 0
    assert isinstance(imp.data['daily_agg_by_hour'].schema["IMPRESSION_HOUR"].dataType, IntegerType)
    assert isinstance(imp.data['daily_agg_by_hour'].schema["CAMPAIGN_ID"].dataType, IntegerType)
    