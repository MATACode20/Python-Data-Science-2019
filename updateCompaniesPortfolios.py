import pandas as pd
from yahoo_finance_hdd import YahooFinance, Parameters
import datetime as dt
import pandas_datareader.data as web
import pymysql
from datetime import date as dt2

start = dt.datetime(2016,11,7)
end = dt.datetime(2019,11,23)

def DownloadPrice (ticket):
    try:
        df = web.DataReader(ticket,"yahoo",start,end)
        return df
    except:
        pass

# Open database connection
db = pymysql.connect("localhost","root","","stockexchange")

# prepare a cursor object using cursor() method
cursor = db.cursor()

companies = ["AMGN","BKNG","CTAS","MELI","IDXX","CME","ZTS","DD","ECL","EQIX",
"LHX","AZO","TDG","CTVA","MTD","TFX","AMCR","COO","BKR","MKTX","NVR","IEX","RE","GL","FRT","HII","AIZ","IPGP"]

cantidad = len(companies)

for x in companies:

    stock  = DownloadPrice(x)

    if (stock is None)==False:
        print("ticket de la compañia {} ".format(x))
        print(" faltan {} compañias ".format(cantidad))
        cantidad = cantidad - 1
        dates   =  stock.index.values
        lows    = (stock.loc[:,"Low"]).values.tolist()
        opens   = (stock.loc[:,"Open"]).values.tolist()
        highs   = (stock.loc[:,"High"]).values.tolist()
        closes  = (stock.loc[:,"Close"]).values.tolist()
        volumes = (stock.loc[:,"Volume"]).values.tolist()
        
        for i in range(len(dates)):
            try:
                # Prepare SQL query to INSERT a record into the database.
                sql = "INSERT INTO price(company, date, open, low , high,  close, volume) \
                VALUES ('{}','{}',{},{},{},{},{})".format(x, dates[i], opens[i], highs[i], lows[i], closes[i],  volumes[i])
                # Execute the SQL command
                cursor.execute(sql)
                # Commit your changes in the database
                db.commit()
            except (pymysql.err.OperationalError, pymysql.err.InternalError, pymysql.err.IntegrityError) as e:
                print("Ocurrió un error al conectar: Por favor intente resolverlo con la siguiente especificación ", e)
                pass
    else:
        pass
# desconectar del servidor
db.close() 

