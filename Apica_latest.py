# this is our basic python connection test script from the docs
#!/usr/bin/env python
import sys
from snowflake.connector import connect
import logging
import json
import pandas as pd

USER = "SVC_SNOWDEV_EDPDEV_APICA"
PASSWORD = "sn0wflak3_SF@apicadev9"
ACCOUNT = "deltadentalins-dev"

conn = connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    #warehouse=WAREHOUSE, --optional if you want to use WH for running other queries as part of script
    session_parameters={
        'QUERY_TAG': 'APICA',
    }
    )


sql1 = "SELECT CURRENT_TIMESTAMP() AS CURRENT_TIME, CURRENT_USER AS CONNECTED_USER"

cursor = conn.cursor()
cursor.execute(sql1)

df = cursor.fetch_pandas_all()
#print(df)

current_time = df.CURRENT_TIME[0]
current_user = df.CONNECTED_USER[0]

#json = df.to_json()
#print(json)

json_return = {
  'returncode': 0,
  'start_time': current_time,
  'value': 0,
  'message': 'HTTP Call completed with status OK',
  'metrics': {
    'duration': 0,
    'content_size': 1256,
    'header_count': 11,
    'http_status': 200
  }
}
print(json.dumps(json_return))
cursor.close()
#print("Closed Snowflake connection")
