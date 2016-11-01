#####################################################
# AFFINITAS DWH TASK by ONUR SARKAN
# This script is main script of task package.
# All ETL jobs are executed in this file.
#####################################################
import extr_lib
import transform_lib
import load_lib
import cfg_lib
import sys

try:
    # Location info  are collected for ETL from location dimension table.
    conn = cfg_lib.dbConnect()
    cur = conn.cursor()
    cur.execute("""SELECT station_id, data_url, city_id FROM DWH_AFFINITAS.dim_location""")
    rows = cur.fetchall()

    print "Data extraction begin:"
    for row in rows:
        print "Data is loading for ", row[1]," \n"
        #Unfortunately, historic files are not available anymore:
        #extr_lib.extractHistoricDataFromUrl(row[1], row[0])
        extr_lib.extractWeatherData_3h(row[2], row[0])


    print "Data transform begin:"
    transform_lib.transformHistoricData()
    transform_lib.transformdWeatherData_3h()
    transform_lib.transformdWeatherData_daily()
    transform_lib.transformdWeatherData_monthly()


    print "Data load begin:"
    load_lib.loadHistoricData()
    load_lib.loadWeatherData_3h()
    load_lib.loadWeatherData_daily()
    load_lib.loadWeatherData_monthly()

    print "Old data cleaning:"
    cur.execute("""DELETE FROM DWH_AFFINITAS.fct_weather where log_type_id=3 AND date_id<to_char(current_date-7, 'YYYYMMDDHH')::integer""")
    cur.execute("""DELETE FROM DWH_AFFINITAS.fct_weather where log_type_id=2 AND date_id<to_char(current_date-30, 'YYYYMMDD')::integer""")
    cur.close()
    conn.close()


except:
   print "Unexpected errorStation:", sys.exc_info()[0]