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
    "returncode": 0,
    "current_time": current_time,
    "current_user": current_user,
    "message": "URL call returned status code: " + str(response.status_code),
    "unit": "ms",
    "value": response.status.code,

}
print(json.dumps(json_return))
cursor.close()
#print("Closed Snowflake connection")
