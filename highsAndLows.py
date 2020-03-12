import pandas as pd
from yahoo_finance_hdd import YahooFinance, Parameters
import datetime as dt
from datetime import date
import pandas_datareader.data as web
import pymysql
import smtplib
from SendEmail import sendEmail 

companiesHigh15 = ""
companiesHighM = ""
companiesHigh2M = ""
companiesHigh4M = ""
companiesHigh6M = ""
companiesHigh1A = ""
companiesHighHys = ""

companiesLow15 = ""
companiesLowM = ""
companiesLow2M = ""
companiesLow4M = ""
companiesLow6M = ""
companiesLow1A = ""
companiesLowHys = ""

contHigh15 = 0
contHighM = 0
contHigh2M = 0
contHigh4M = 0
contHigh6M = 0
contHigh1A = 0
contHighHys = 0

contLow15 = 0
contLowM = 0
contLow2M = 0
contLow4M = 0
contLow6M = 0
contLow1A = 0
contLowHys = 0

def enviarMensaje(message):
    messageT = ""
    messageT = messageT + message + "\n\n"
    
    messageT  = messageT + "Lista de compañias que el día {} lograron el High más alto Historicamente( {} ) : ".format(date.today(),contHighHys)
    messageT = messageT + "\n" + companiesHighHys + "\n"

    messageT  = messageT + "Lista de compañias que el día {} lograron el High más alto en el ultimo año( {} ) : ".format(date.today(),contHigh1A)
    messageT = messageT + "\n" + companiesHigh1A + "\n"

    messageT  = messageT + "Lista de compañias que el día {} lograron el High más alto en los últimos 6 meses( {} ) : ".format(date.today(),contHigh6M)
    messageT = messageT + "\n" + companiesHigh6M + "\n"

    messageT  = messageT + "Lista de compañias que el día {} lograron el High más alto en los últimos 4 meses( {} ) : ".format(date.today(),contHigh4M)
    messageT = messageT + "\n" + companiesHigh4M + "\n"

    messageT  = messageT + "Lista de compañias que el día {} lograron el High más alto en los últimos 2 meses( {} ) : ".format(date.today(),contHigh2M)
    messageT = messageT + "\n" + companiesHigh2M + "\n"

    messageT  = messageT + "Lista de compañias que el día {} lograron el High más alto en el último mes ( {} ) : ".format(date.today(),contHighM)
    messageT = messageT + "\n" + companiesHighM + "\n"

    messageT  = messageT + "Lista de compañias que el día {} lograron el High más alto en los últimos 15 días( {} ) : ".format(date.today(),contHigh15)
    messageT = messageT + "\n" + companiesHigh15 + "\n\n\n"

    messageT  = messageT + "Lista de compañias que el día {} lograron el Low más bajo Historicamente( {} ) : ".format(date.today(),contLowHys)
    messageT = messageT + "\n" + companiesLowHys + "\n"

    messageT  = messageT + "Lista de compañias que el día {} lograron el Low más bajo en el último año( {} ) : ".format(date.today(),contLow1A)
    messageT = messageT + "\n" + companiesLow1A + "\n"

    messageT  = messageT + "Lista de compañias que el día {} lograron el Low más bajo en los últimos 6 meses( {} ) : ".format(date.today(),contLow6M)
    messageT = messageT + "\n" + companiesLow6M + "\n"

    messageT  = messageT + "Lista de compañias que el día {} lograron el Low más bajo en los últimos 4 meses( {} ) : ".format(date.today(),contLow4M)
    messageT = messageT + "\n" + companiesLow4M + "\n"

    messageT  = messageT + "Lista de compañias que el día {} lograron el Low más bajo en los últimos 2 meses( {} ) : ".format(date.today(),contLow2M)
    messageT = messageT + "\n" + companiesLow2M + "\n"

    messageT  = messageT + "Lista de compañias que el día {} lograron el Low más bajo en el último mes( {} ) : ".format(date.today(),contLowM)
    messageT = messageT + "\n" + companiesLowM + "\n"

    messageT  = messageT + "Lista de compañias que el día {} lograron el Low más bajo en los últimos 15 dias( {} ) : ".format(date.today(),contLow15)
    messageT = messageT + "\n" + companiesLow15 + "\n"
    try:
        sendEmail(messageT,date.today(),"Prices And HighLows")
    except smtplib.SMTPServerDisconnected as e:
        print("paila",e)
        enviarMensaje(message)
    

def HihgAndLows(message):
    # Open database connection
    db = pymysql.connect("localhost","root","","stockexchange")

    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    sqlCompanies = "select company.ticket from company where company.status=1"

    cursor.execute(sqlCompanies)

    companies = cursor.fetchall()

    contador = 1
    for x in companies:
        
        
        contador = contador + 1
        mini = 0
        maxi = 0
        ticket = x[0]

        
        sql = """
                select min(low),max(high)
                from price
                where company='{}'
        """.format(ticket)

        cursor.execute(sql)

        result = list(cursor.fetchall())

        sqlO = "select high from price where price.company='{}' and price.date='{}'".format(ticket,date.today())

        cursor.execute(sqlO)

        highT = list(cursor.fetchall())

        sqlO2 = "select low from price where price.company='{}' and price.date='{}'".format(ticket,date.today())

        cursor.execute(sqlO2)

        lowT = list(cursor.fetchall())
        print(highT)

        if (result[0] is None)==False :
            mini = result[0][0]
            maxi = result[0][1]

            dates = (360,180,120,60,30,15)
            for y in dates:
                highB = 0
                lowB = 0
                typeBH = ""
                typeBL = ""
                sqlH = """select max(high) from ( select high from price where price.company='{}' order by date desc limit {}) r""".format(ticket,y)
                sqlL = """select min(low) from ( select low from price where price.company='{}' order by date desc limit {}) r""".format(ticket,y)
                cursor.execute(sqlH)

                resultH = list(cursor.fetchall())
                cursor.execute(sqlL)

                resultL = list(cursor.fetchall())
                
                if len(resultH)!=0 and len(highT)!=0:
                    
                    print((highT[0][0])>(maxi))
                    print((highT[0][0])>(resultH[0][0]))
                    print((lowT[0][0])<(mini))
                    print((lowT[0][0])<(resultL[0][0]))
                    print(str(y))

                    if (highT[0][0])>maxi:
                        print("entro hh")
                        typeBH = typeBH + "Historycal"
                        highB = highB + 1
                        global contHighHys
                        contHighHys = contHighHys + 1
                        global companiesHighHys
                        companiesHighHys = companiesHighHys + ticket + " "
                        break
                    elif (highT[0][0])>(resultH[0][0]):   
                        print("entro periodos h")
                        if y == 360:
                            print("entro H")
                            typeBH = typeBH + "Year"
                            highB = highB + 1
                            global contHigh1A
                            contHigh1A = contHigh1A + 1
                            global companiesHigh1A
                            companiesHigh1A = companiesHigh1A + ticket + " "
                            break
                        elif y == 180:
                            print("entro H")
                            typeBH = typeBH + "6Months"
                            highB = highB + 1
                            global contHigh6M
                            contHigh6M = contHigh6M + 1
                            global companiesHigh6M
                            companiesHigh6M = companiesHigh6M + ticket + " "
                            break
                        elif y == 120:
                            print("entro H")
                            typeBH = typeBH + "4Months"
                            highB = highB +  1
                            global contHigh4M
                            contHigh4M = contHigh4M + 1
                            global companiesHigh4M
                            companiesHigh4M = companiesHigh4M + ticket + " "
                            break
                        elif y == 60:
                            print("entro H")
                            typeBH = typeBH +  "2Months"
                            highB = highB + 1
                            global contLow2M
                            contHigh2M = contHigh2M + 1
                            global companiesHigh2M
                            companiesHigh2M = companiesHigh2M + ticket + " "
                            break
                        elif y == 30:
                            print("entro H")
                            typeBH = typeBH + "1Month"
                            highB = highB + 1
                            global contHighM
                            contHighM = contHighM + 1
                            global companiesHighM
                            companiesHighM = companiesHighM + ticket + " "
                            break
                        elif y == 15:
                            print("entro H")
                            typeBH = typeBH + "15Days"
                            highB = highB + 1
                            global contHigh15
                            contHigh15 = contHigh15 + 1
                            global companiesHigh15
                            companiesHigh15 = companiesHigh15 + ticket + " "
                            break
                if len(resultL)!=0 and len(lowT)!=0:
                    if lowT[0][0]<mini:
                        print("entroHL")
                        typeBL = typeBL + "Historycal"
                        lowB = lowB + 1
                        global contLowHys
                        contLowHys = contLowHys + 1
                        global companiesLowHys
                        companiesLowHys = companiesLowHys + ticket + " "
                        break  
                    elif (lowT[0][0]<(resultL[0][0]))==True:
                        print("entra periodos")
                        if y == 360:
                            typeBL = typeBL +  "Year"
                            print("entro")
                            lowB = 1
                            global contLow1A
                            contLow1A = contLow1A + 1
                            global companiesLow1A
                            companiesLow1A = companiesLow1A + ticket  + " "
                            break
                        elif y == 180:
                            print("entro")
                            typeBL = typeBL +  "6Months"
                            lowB = 1
                            global contLow6M
                            contLow6M = contLow6M + 1
                            global companiesLow6M
                            companiesLow6M = companiesLow6M + ticket + " "
                            break
                        elif y == 120:
                            print("entro")
                            typeBL = typeBL +  "4Months"
                            lowB = 1
                            global contLow4M
                            contLow4M = contLow4M + 1
                            global companiesLow4M
                            companiesLow4M = companiesLow4M + ticket + " "
                            break
                        elif y == 60:
                            print("entro")
                            typeBL = typeBL + "2Months"
                            lowB = 1
                            global contLow2M
                            contLow2M = contLow2M + 1
                            global companiesLow2M
                            companiesLow2M = companiesLow2M + ticket + " "
                            break
                        elif y == 30:
                            print("entro")
                            typeBL = typeBL + "1Month"
                            lowB = lowB + 1
                            global contLowM
                            contLowM = contLowM + 1
                            global companiesLowM
                            companiesLowM = companiesLowM + ticket + " "
                            break
                        elif y == 15:
                            print("entro")
                            typeBL = typeBL +  "15Days"
                            lowB = lowB + 1 
                            global contLow15
                            contLow15 = contLow15 + 1
                            global companiesLow15
                            companiesLow15 = companiesLow15 + ticket + " "
                            break
                        else:
                            pass
                    else:
                        pass
                else:
                    pass

            print("type B H " + typeBH )
            print("type B L " + typeBL)
            if len(typeBH) != 0:
                try:
                    sqlHigh = """insert into reporthl(ticket,date,high,low,type) values('{}','{}',{},{},'{}')""".format(ticket,date.today(),highB,lowB,typeBH)
                    cursor.execute(sqlHigh)
                    db.commit()
                except (pymysql.err.OperationalError, pymysql.err.InternalError, pymysql.err.IntegrityError) as e:
                    print("ocurrió un problema (High): ", e)
            elif len(typeBL) != 0:
                try:
                    sqlLow = """insert into reporthl(ticket,date,high,low,type) values('{}','{}',{},{},'{}')""".format(ticket,date.today(),highB,lowB,typeBL)
                    cursor.execute(sqlLow)
                    db.commit()
                except (pymysql.err.OperationalError, pymysql.err.InternalError, pymysql.err.IntegrityError) as e:
                    print("ocurrió un problema (Low) : ", e)
                    pass
            else:
                pass
        else:
            pass
            print("paila")

    db.close()

    enviarMensaje(message)
