# this is our basic python connection test script from the docs
#!/usr/bin/env python
import snowflake.connector
import logging
 
#logging.basicConfig(filename='/app/snowflake_python_connector.log',level=logging.DEBUG)
 
# Gets the version
ctx = snowflake.connector.connect(
user='SVC_SNOWDEV_EDPDEV_APICA',
password='sn0wflak3_SF@apicadev9',
account='deltadentalins-dev'
)
cs = ctx.cursor()
try:
    cs.execute("SELECT current_timestamp()")
    one = cs.fetchone()
    print(one[0])
finally:
    cs.close()
    ctx.close()
    print("Closed Snowflake connection")
