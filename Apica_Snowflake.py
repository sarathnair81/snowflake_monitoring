# this is our basic python connection test script from the docs
#!/usr/bin/env python
import sys
import snowflake.connector
from snowflake.connector import connect
import logging
import json
import pandas as pd

USER = "SVC_SNOWDEV_EDPDEV_APICA"
PASSWORD = "sn0wflak3_SF@apicadev9"
ACCOUNT = "<account>"
DB = "<db>"
WAREHOUSE = "<wh>"

conn = connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    database=DB,
    warehouse=WAREHOUSE,
    session_parameters={
        'QUERY_TAG': 'APICA',
    }
    )


sql1 = "SELECT CURRENT_TIMESTAMP() AS EVENT_TIMESTAMP, CURRENT_USER AS USER_NAME"

sql2 = "select * from table(information_schema.login_history_by_user(user_name => 'HAREESH', result_limit => 1)) order by event_timestamp"

cursor = conn.cursor()
cursor.execute(sql2)
df = cursor.fetch_pandas_all()
# print(df)

return_code = 0
current_time = df.EVENT_TIMESTAMP[0]
current_user = df.USER_NAME[0]
is_success = df.IS_SUCCESS[0]
message = ("IS_LOGIN_SUCCESSFUL: " + is_success )
value = df.ERROR_CODE[0]


if value is None:
    value_code = 200
else:
    value_code = value

#json = df.to_json()
#print(json)

json_return = {
    "returncode": return_code,
    "start_time": current_time,
    "end_time": current_time,
    "current_user": current_user,
    "message": message,
    "value": value_code
}
print(json_return)
cursor.close()
#print("Closed Snowflake connection")
