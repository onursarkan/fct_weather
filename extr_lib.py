#####################################################
# AFFINITAS DWH TASK by ONUR SARKAN
# Data extraction methods are implemented in this script.
#####################################################
import cfg_lib
import sys
import urllib2
import urllib
import json

########################################################################################
# This method extract historical weather data from given url.
# url: This is url of raw historical weather data.
# stationId: ID of meteorological station.
def extractHistoricDataFromUrl(url,stationId):
    try:
        conn = cfg_lib.dbConnect()
        cur = conn.cursor()

        # To avoid from http 403 error, artificial header should be added to read request:
        hdr = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
            'Accept-Encoding': 'none',
            'Accept-Language': 'en-US,en;q=0.8',
            'Connection': 'keep-alive'}

        # Data read from source URL:
        readRequest = urllib2.Request(url, headers=hdr)
        rawData = urllib2.urlopen(readRequest)

        # To avoid from data duplications, data is cleaned for related meteorological station:
        cur.execute("DELETE FROM DWH_EXTR.monthly_historic_weather WHERE station_id = %s", (stationId,))
        conn.commit()

        # Raw historical weather data are inserted to extraction table:
        for line in rawData:
            x = line.split()

            if x[0].isdigit() and int(x[0])>1700 and int(x[0])<2100:
                try:
                    cur.execute(
                        "INSERT INTO DWH_EXTR.monthly_historic_weather (station_id,year,month,max_temp,min_temp,af_days,rain_mm,sun_hours,etl_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)",
                        (stationId, x[0], x[1], x[2], x[3], x[4], x[5], x[6],))
                except:
                    print "Error: ", x[0]," ", x[1]
                    print "Unexpected errorStation:", sys.exc_info()[0]
                    # Extraction errors are saved for further manuel corrections:
                    cur.execute("INSERT INTO DWH_EXTR.monthly_historic_weather_error (station_id,year,month,raw_data,etl_date) VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)",
                    (stationId, x[0], x[1], line,))
            conn.commit()
        cur.close()
        conn.close()
        return 1
    except:
        print "Unexpected error:", sys.exc_info()[0]
        return 0
########################################################################################

########################################################################################
# This method extract current weather data from json API.
# city_id: This is id of related location which allow us json query
# stationId: ID of meteorological station.
def extractWeatherData_3h(city_id,stationId):
    try:
        conn = cfg_lib.dbConnect()
        cur = conn.cursor()

        # To avoid from data duplications, data is cleaned for related meteorological station:
        cur.execute("DELETE FROM DWH_EXTR.current_weather WHERE station_id = %s", (stationId,))
        conn.commit()

        # json url is created:
        url = "http://api.openweathermap.org/data/2.5/forecast/city?id="+str(city_id)+"&APPID=b754a060a05af2d1885f0e7f68d96aaa"
        response = urllib.urlopen(url)
        data = json.loads(response.read())

        for x in data[u'list']:
            try:
                temp_min = x['main'][u'temp_min']
            except:
                temp_min = 0
            try:
                temp_max = x['main'][u'temp_max']
            except:
                temp_max = 0
            try:
                rain_mm = x['rain']['3h']
            except:
                rain_mm = 0
            try:
                cur.execute(
                    "INSERT INTO DWH_EXTR.current_weather(station_id,date_str,max_temp,min_temp,rain_mm,etl_date) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)",
                    (stationId,x[u'dt_txt'],temp_max, temp_min, rain_mm,))
            except:
                print "Unexpected errorStation:", sys.exc_info()[0]
        conn.commit()
        cur.close()
        conn.close()
        return 1
    except:
        print "Unexpected error:", sys.exc_info()[0]
        return 0
########################################################################################