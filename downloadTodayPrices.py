import pandas as pd
from yahoo_finance_hdd import YahooFinance, Parameters
from datetime import datetime as dt
from datetime import date as dt2
import datetime as tt
import pandas_datareader.data as web
from SendEmail import sendEmail
import pymysql
import holidays
from highsAndLows import HihgAndLows
from PortfoliosDaily import portfoliosDaily
from PortfoliosIndexesDaily import portfoliosIndexesDaily

us_holidays = holidays.UnitedStates() 

holidays = holidays.UnitedStates(years = 2019).keys()

companiesDelet = list()

start = dt2.today()
end = dt2.today()
flag2 = False
flag3 = False
flag = True
def DownloadPrice (ticket,flag2):
    try:
        df = web.DataReader(ticket,"yahoo",start,end)
        return df
    except:
        if(flag2==True):
            print("la compañia no existe " + ticket)
            sql = "update company set status=0,deleted_date='{}' where ticket='{}'".format(start,ticket)
            cursor.execute(sql)
            db.commit() 
            global cont
            cont = cont+1
        else:
            companiesDelet.append(ticket)

# Open database connection
db = pymysql.connect("localhost","root","","stockexchange")

# prepare a cursor object using cursor() method
cursor = db.cursor()

sql1 = "select company.ticket from company where status = 1"

cursor.execute(sql1)

companies = cursor.fetchall()
cont = 0

def insertData(companies,flag2):

   
    contador = 1
    

    canti = len(companies)
    if start.weekday() < 5 and  (start in holidays)==False :
        global flag3
        flag3 = True
        for x in companies:
            if  contador<=len(companies):
                ticket = ""
                if(flag2==True):
                    ticket = x
                    stock  = DownloadPrice(ticket,flag2)
                    print(ticket)
                    print( contador," de ",canti)
                    contador = contador + 1
                else:
                    ticket = x[0]
                    stock  = DownloadPrice(ticket,flag2)
                    print(ticket)
                    print( contador," de ",canti)
                    contador = contador + 1
                if (stock is None)==False:
                    
                    dates   =  stock.index.values
                    lows    = (stock.loc[:,"Low"]).values.tolist()
                    opens   = (stock.loc[:,"Open"]).values.tolist()
                    highs   = (stock.loc[:,"High"]).values.tolist()
                    closes  = (stock.loc[:,"Close"]).values.tolist()
                    volumes = (stock.loc[:,"Volume"]).values.tolist()
                    
                    for i in range(len(dates)):
                        # Prepare SQL query to INSERT a record into the database.
                        try:
                            sql = "INSERT INTO price(company, date, open, high , low,  close, volume) \
                            VALUES ('{}','{}',{},{},{},{},{})".format(x[0], dates[i], opens[i], highs[i], lows[i], closes[i],  volumes[i])
                        
                            # Execute the SQL command
                            cursor.execute(sql)
                            # Commit your changes in the database
                            db.commit()
                        except (pymysql.err.OperationalError, pymysql.err.InternalError) as e:
                            print("Ocurrió un error al conectar: Por favor intente resolverlo con la siguiente especificación ", e)
                            db.rollback()
                        except (pymysql.err.IntegrityError,pymysql.err.InterfaceError):
                            pass
                else:
                    print(" no se encontró stock")
                    pass
            else:
                break
    else:
        print("Hoy la bolsa no trabaja")
        pass


insertData(companies,flag2)
flag2 = True
insertData(companiesDelet,flag2)

 # desconectar del servidor
db.close()

if flag2==True and flag3==True:
        message ="""Se han cargado los precios de ({}) compañias correctamente el día {} \n
 Se intentaron inactivar ({}) compañias
 Número de compañias inactivadas hoy ({}) : """.format(len(companies),start,len(companiesDelet),cont)
        cadena = ""
        for x in companiesDelet:
            cadena = cadena + x + " "
        message = message + "\n" + cadena

        HihgAndLows(message)
        portfoliosDaily()
        portfoliosIndexesDaily()