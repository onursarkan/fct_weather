#####################################################
# AFFINITAS DWH TASK by ONUR SARKAN
# Data load methods are implemented in this script.
#####################################################
import cfg_lib
import sys

########################################################################################
# This method load historical weather data into target DWH table.
def loadHistoricData():
    try:
        conn = cfg_lib.dbConnect()
        cur = conn.cursor()
        # To avoid from data duplications, data is cleaned:
        cur.execute("DELETE FROM DWH_AFFINITAS.fct_weather WHERE log_type_id=1 and (station_id,date_id) IN "
                    "(SELECT station_id,date_id FROM DWH_STG.stg_historic_weather)")
        conn.commit()
        cur.execute("INSERT INTO DWH_AFFINITAS.fct_weather(station_id,log_type_id,date_id,max_temp,min_temp,af_days,rain_mm,sun_hours,etl_date) "
                    "SELECT station_id,1,date_id,max_temp,min_temp,af_days,rain_mm,sun_hours,CURRENT_TIMESTAMP FROM DWH_STG.stg_historic_weather")
        conn.commit()

        cur.close()
        conn.close()
        return 1
    except:
        print "Unexpected error:", sys.exc_info()[0]
        return 0
########################################################################################

########################################################################################
# This method load 3h weather data into target DWH table.
def loadWeatherData_3h():
    try:
        conn = cfg_lib.dbConnect()
        cur = conn.cursor()

        # To avoid from data duplications, data is cleaned:
        cur.execute("DELETE FROM DWH_AFFINITAS.fct_weather WHERE log_type_id=3 and (station_id,date_id) IN"
                    "(SELECT station_id,date_id FROM DWH_STG.stg_current_weather_3h)")
        cur.execute("INSERT INTO DWH_AFFINITAS.fct_weather(station_id,log_type_id,date_id,max_temp,min_temp,rain_mm,etl_date)"
                    "SELECT station_id,3,date_id,max_temp,min_temp,rain_mm,CURRENT_TIMESTAMP FROM DWH_STG.stg_current_weather_3h")
        conn.commit()

        cur.close()
        conn.close()
        return 1
    except:
        print "Unexpected error:", sys.exc_info()[0]
        return 0
########################################################################################

########################################################################################
# This method load daily weather data into target DWH table.
def loadWeatherData_daily():
    try:
        conn = cfg_lib.dbConnect()
        cur = conn.cursor()

        # To avoid from data duplications, data is cleaned:
        cur.execute("DELETE FROM DWH_AFFINITAS.fct_weather WHERE log_type_id=2 and (station_id,date_id) IN"
                    "(SELECT station_id,date_id FROM DWH_STG.stg_current_weather_daily)")
        cur.execute("INSERT INTO DWH_AFFINITAS.fct_weather(station_id,log_type_id,date_id,max_temp,min_temp,af_days,rain_mm,etl_date)"
                    "SELECT station_id,2,date_id,max_temp,min_temp,af_days,rain_mm,CURRENT_TIMESTAMP FROM DWH_STG.stg_current_weather_daily")
        conn.commit()

        cur.close()
        conn.close()
        return 1
    except:
        print "Unexpected error:", sys.exc_info()[0]
        return 0
########################################################################################

########################################################################################
# This method load monthly weather data into target DWH table.
def loadWeatherData_monthly():
    try:
        conn = cfg_lib.dbConnect()
        cur = conn.cursor()

        # To avoid from data duplications, data is cleaned:
        cur.execute("DELETE FROM DWH_AFFINITAS.fct_weather WHERE log_type_id=1 and (station_id,date_id) IN"
                    "(SELECT station_id,date_id FROM DWH_STG.stg_current_weather_monthly)")
        cur.execute("INSERT INTO DWH_AFFINITAS.fct_weather(station_id,log_type_id,date_id,max_temp,min_temp,af_days,rain_mm,etl_date)"
                    "SELECT station_id,1,date_id,max_temp,min_temp,af_days,rain_mm,CURRENT_TIMESTAMP FROM DWH_STG.stg_current_weather_monthly")
        conn.commit()

        cur.close()
        conn.close()
        return 1
    except:
        print "Unexpected error:", sys.exc_info()[0]
        return 0
########################################################################################



