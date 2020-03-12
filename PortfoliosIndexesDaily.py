import pymysql
from datetime import datetime,timedelta
import math
from datetime import date as dt

def portfoliosIndexesDaily():

    db = pymysql.connect("localhost","root","","stockexchange")

    cursor = db.cursor()

    today = dt.today()
    yesterday = today - datetime.timedelta(days=1)


    sqlIndustries = "select id from industry"
    cursor.execute(sqlIndustries)
    industries = cursor.fetchall()

    sqlSectors = "select id from sector"
    cursor.execute(sqlSectors)
    sectors = cursor.fetchall()

    sqlIndexesNames = "select id from indexesname"
    cursor.execute(sqlIndexesNames)
    indexesNames = cursor.fetchall()
    performance = 0
    performance100 = 0

    for index in indexesNames:
        for industry in industries:

            sql = """SELECT portfolioindustries.indexValue
                FROM portfolioindustries 
                JOIN  industry ON industry.id=portfolioindustries.industryid
                WHERE portfolioindustries.indexType={} AND industry.id={} AND portfolioindustries.date="{}"
                ORDER BY portfolioindustries.date """.format(index[0],industry[0],today)
            cursor.execute(sql)
            todayResult = cursor.fetchall()


            sql = """SELECT portfolioindustries.indexValue
                FROM portfolioindustries 
                JOIN  industry ON industry.id=portfolioindustries.industryid
                WHERE portfolioindustries.indexType={} AND industry.id={} AND portfolioindustries.date="{}"
                ORDER BY portfolioindustries.date """.format(index[0],industry[0],yesterday)
            cursor.execute(sql)
            yesterdayResult = cursor.fetchall()


            if len(todayResult) != 0 and len(yesterdayResult) != 0:
                performance = math.log(todayResult[0][0]/yesterdayResult[0][0])
                regla = (performance*100)/todayResult[0][0]
                performance100 = regla + 100
                sql1 = "insert into dailyperformanceindexindustries(industryid,indextype,indexvalue,date,performance,performance100) values({},{},{},'{}',{},{})".format(industry[0],index[0],todayResult[0][0],today,performance,performance100)
                cursor.execute(sql1)
                db.commit()
                x = x+1
            else:
                print("no existen")

    for index in indexesNames:
        for sector in sectors:
            
            sql = """SELECT portfoliosectors.indexValue
                FROM portfoliosectors 
                JOIN  sector ON sector.id=portfoliosectors.sector
                WHERE portfoliosectors.indexType={} AND sector.id={} AND portfoliosectors.date="{}"
                ORDER BY portfoliosectors.date""".format(index[0],sector[0],today)
            
            cursor.execute(sql)
            todayResult = cursor.fetchall()

            sql = """SELECT portfoliosectors.indexValue
                FROM portfoliosectors 
                JOIN  sector ON sector.id=portfoliosectors.sector
                WHERE portfoliosectors.indexType={} AND sector.id={} AND portfoliosectors.date="{}"
                ORDER BY portfoliosectors.date""".format(index[0],sector[0],yesterday)
            
            cursor.execute(sql)
            yesterdayResult = cursor.fetchall()
            
            if len(todayResult) != 0 and len(yesterdayResult) != 0:
                performance = math.log(todayResult[0][0]/yesterdayResult[0][0])
                regla = (performance*100)/todayResult[0][0]
                performance100 = regla + 100
                sql1 = "insert into dailyperformanceindexsectors(sectorid,indextype,indexvalue,date,performance,performance100) values({},{},{},'{}',{},{})".format(sector[0],index[0],todayResult[0][0],today,performance,performance100)
                cursor.execute(sql1)
                db.commit()

    db.close()