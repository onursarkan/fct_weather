#####################################################
# AFFINITAS DWH TASK by ONUR SARKAN
# Common methods are implemented in this script.
#####################################################
import psycopg2
import sys

########################################################################################
# This method create db connection object.
# If target db is changed, you need to change just one method.
def dbConnect():
    try:
        # Connection is created for target relational db:
        conn = psycopg2.connect(
            "dbname='DWH' user='SYS' host='petsdeli.cskwxu9jcmfj.eu-central-1.rds.amazonaws.com' port = 5432 password='12345678'")
        return conn
    except:
        print "Unexpected error:", sys.exc_info()[0]
        return 0