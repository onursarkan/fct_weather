#####################################################
# AFFINITAS DWH TASK by ONUR SARKAN
# Data transform methods are implemented in this script.
#####################################################
import cfg_lib
import sys

########################################################################################
# This method prepare historical weather data in staging db schema.
def transformHistoricData():
    try:
        conn = cfg_lib.dbConnect()
        cur = conn.cursor()

        # To avoid from data duplications, data is cleaned for related meteorological station from target DWH fact table:
        cur.execute("DELETE FROM DWH_STG.stg_historic_weather ")
        conn.commit()

        cur.execute("INSERT INTO DWH_STG.stg_historic_weather(station_id,date_id,max_temp,min_temp,af_days,rain_mm,sun_hours,etl_date)"
            "SELECT station_id,(year||lpad(month,2,'0'))::integer,(CASE WHEN max_temp ~ E'^([0-9]+[.]?[0-9]*|[.][0-9]+)$' THEN max_temp ELSE NULL END)::numeric,"
            "(CASE WHEN min_temp ~ E'^([0-9]+[.]?[0-9]*|[.][0-9]+)$' THEN min_temp ELSE NULL END)::numeric,(CASE WHEN af_days ~ E'^\\d+$' THEN af_days ELSE NULL END)::integer,"
            "(CASE WHEN rain_mm ~ E'^([0-9]+[.]?[0-9]*|[.][0-9]+)$' THEN rain_mm ELSE NULL END)::numeric,(CASE WHEN sun_hours ~ E'^([0-9]+[.]?[0-9]*|[.][0-9]+)$' THEN sun_hours ELSE NULL END)::numeric,"
            "CURRENT_TIMESTAMP FROM DWH_EXTR.monthly_historic_weather ")
        conn.commit()

        cur.close()
        conn.close()
        return 1
    except:
        print "Unexpected error:", sys.exc_info()[0]
        return 0
########################################################################################

########################################################################################
# This method prepare 3h weather data in staging db schema.
def transformdWeatherData_3h():
    try:
        conn = cfg_lib.dbConnect()
        cur = conn.cursor()

        # To avoid from data duplications, data is cleaned for related meteorological station from target DWH fact table:
        cur.execute("DELETE FROM DWH_STG.stg_current_weather_3h ")
        conn.commit()

        try:
            # Data is transformed and loaded to target DWH fact table:
            cur.execute(
                "INSERT INTO DWH_STG.stg_current_weather_3h(station_id,date_id,max_temp,min_temp,rain_mm,etl_date) "
                "SELECT station_id,(substr(date_str,1,4)||substr(date_str,6,2)||substr(date_str,9,2)||substr(date_str,12,2))::integer,max_temp::numeric-273,"
                "min_temp::numeric-273,rain_mm::numeric,CURRENT_TIMESTAMP FROM DWH_EXTR.current_weather ")
            conn.commit()
        except:
            print "Unexpected errorStation:", sys.exc_info()[0]
        cur.close()
        conn.close()
        return 1
    except:
        print "Unexpected error:", sys.exc_info()[0]
        return 0
########################################################################################

########################################################################################
# This method prepare daily weather data in staging db schema.
def transformdWeatherData_daily():
    try:
        conn = cfg_lib.dbConnect()
        cur = conn.cursor()

        # To avoid from data duplications, data is cleaned for related meteorological station from target DWH fact table:
        cur.execute("DELETE FROM DWH_STG.stg_current_weather_daily ")
        conn.commit()

        try:
            # Data is transformed and loaded to target DWH fact table:
            cur.execute(
                "INSERT INTO DWH_STG.stg_current_weather_daily(station_id,date_id,max_temp,min_temp,af_days,rain_mm,etl_date) "
                "SELECT station_id,to_char(current_date-1, 'YYYYMMDD')::integer,MAX(max_temp),MIN(min_temp),CASE WHEN MIN(min_temp) < 0 THEN 1 ELSE 0 END,SUM(rain_mm),CURRENT_TIMESTAMP "
                "FROM DWH_AFFINITAS.fct_weather WHERE log_type_id=3 AND substr(date_id::text,1,8)= to_char(current_date-1, 'YYYYMMDD') GROUP BY station_id ")
            cur.execute(
                "INSERT INTO DWH_STG.stg_current_weather_daily(station_id,date_id,max_temp,min_temp,af_days,rain_mm,etl_date) "
                "SELECT station_id,to_char(current_date, 'YYYYMMDD')::integer,MAX(max_temp),MIN(min_temp),CASE WHEN MIN(min_temp) < 0 THEN 1 ELSE 0 END,SUM(rain_mm),CURRENT_TIMESTAMP "
                "FROM DWH_AFFINITAS.fct_weather WHERE log_type_id=3 AND substr(date_id::text,1,8)= to_char(current_date, 'YYYYMMDD') GROUP BY station_id ")
            cur.execute(
                "INSERT INTO DWH_STG.stg_current_weather_daily(station_id,date_id,max_temp,min_temp,af_days,rain_mm,etl_date) "
                "SELECT station_id,to_char(current_date+1, 'YYYYMMDD')::integer,MAX(max_temp),MIN(min_temp),CASE WHEN MIN(min_temp) < 0 THEN 1 ELSE 0 END,SUM(rain_mm),CURRENT_TIMESTAMP "
                "FROM DWH_AFFINITAS.fct_weather WHERE log_type_id=3 AND substr(date_id::text,1,8)= to_char(current_date+1, 'YYYYMMDD') GROUP BY station_id ")
            cur.execute(
                "INSERT INTO DWH_STG.stg_current_weather_daily(station_id,date_id,max_temp,min_temp,af_days,rain_mm,etl_date) "
                "SELECT station_id,to_char(current_date+2, 'YYYYMMDD')::integer,MAX(max_temp),MIN(min_temp),CASE WHEN MIN(min_temp) < 0 THEN 1 ELSE 0 END,SUM(rain_mm),CURRENT_TIMESTAMP "
                "FROM DWH_AFFINITAS.fct_weather WHERE log_type_id=3 AND substr(date_id::text,1,8)= to_char(current_date+2, 'YYYYMMDD') GROUP BY station_id ")
            cur.execute(
                "INSERT INTO DWH_STG.stg_current_weather_daily(station_id,date_id,max_temp,min_temp,af_days,rain_mm,etl_date) "
                "SELECT station_id,to_char(current_date+3, 'YYYYMMDD')::integer,MAX(max_temp),MIN(min_temp),CASE WHEN MIN(min_temp) < 0 THEN 1 ELSE 0 END,SUM(rain_mm),CURRENT_TIMESTAMP "
                "FROM DWH_AFFINITAS.fct_weather WHERE log_type_id=3 AND substr(date_id::text,1,8)= to_char(current_date+3, 'YYYYMMDD') GROUP BY station_id ")
            cur.execute(
                "INSERT INTO DWH_STG.stg_current_weather_daily(station_id,date_id,max_temp,min_temp,af_days,rain_mm,etl_date) "
                "SELECT station_id,to_char(current_date+4, 'YYYYMMDD')::integer,MAX(max_temp),MIN(min_temp),CASE WHEN MIN(min_temp) < 0 THEN 1 ELSE 0 END,SUM(rain_mm),CURRENT_TIMESTAMP "
                "FROM DWH_AFFINITAS.fct_weather WHERE log_type_id=3 AND substr(date_id::text,1,8)= to_char(current_date+4, 'YYYYMMDD') GROUP BY station_id ")
            conn.commit()

        except:
            print "Unexpected errorStation:", sys.exc_info()[0]
        cur.close()
        conn.close()
        return 1
    except:
        print "Unexpected error:", sys.exc_info()[0]
        return 0
########################################################################################

########################################################################################
# This method prepare monthly weather data in staging db schema.
def transformdWeatherData_monthly():
    try:
        conn = cfg_lib.dbConnect()
        cur = conn.cursor()

        # To avoid from data duplications, data is cleaned:
        cur.execute("DELETE FROM DWH_STG.stg_current_weather_monthly ")
        conn.commit()

        cur.execute(
            "INSERT INTO DWH_STG.stg_current_weather_monthly(station_id,date_id,max_temp,min_temp,af_days,rain_mm,etl_date) SELECT station_id,to_char(current_date, 'YYYYMM')::integer,MAX(max_temp),"
            "MIN(min_temp),SUM(af_days),SUM(rain_mm),CURRENT_TIMESTAMP FROM DWH_AFFINITAS.fct_weather WHERE log_type_id=2 AND substr(date_id::text,1,6)= to_char(current_date, 'YYYYMM') GROUP BY station_id")
        conn.commit()


        cur.close()
        conn.close()
        return 1
    except:
        print "Unexpected error:", sys.exc_info()[0]
        return 0
########################################################################################