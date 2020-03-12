import pandas as pd
import pymysql

db = pymysql.connect("localhost","root","12345","stockexchange")

cursor = db.cursor()

Dow = pd.read_excel("C:\\Users\Marlon\Dropbox\Code TWS\IndicesComponents.xlsx",sheet_name="DOW")
Nasdaq = pd.read_excel("C:\\Users\Marlon\Dropbox\Code TWS\IndicesComponents.xlsx",sheet_name="NASDAQ")
SyP = pd.read_excel("C:\\Users\Marlon\Dropbox\Code TWS\IndicesComponents.xlsx",sheet_name="S&P")
lista = list()
lista2 = list()
lista3 = list()
Dowdoc = pd.DataFrame(Dow.values,columns=Dow.columns)
Nasdaqdoc = pd.DataFrame(Nasdaq.values,columns=Nasdaq.columns)
SyPdoc = pd.DataFrame(SyP.values,columns=SyP.columns)

total = 0
total2 = 0
total3 = 0
for row in Dowdoc.values:
    ticket = row[1]
    print(ticket)
    units = row[4]
    sql = "insert into reportUnits(ticket,units,indiceType) values('{}','{}','DOW')".format(ticket,units)
    cursor.execute(sql)
    db.commit()

for row in Nasdaqdoc.values:
    ticket = row[1]
    print(ticket)
    units = row[4]
    sql = "insert into reportUnits(ticket,units,indiceType) values('{}','{}','NASDAQ')".format(ticket,units)
    cursor.execute(sql)
    db.commit()

for row in SyPdoc.values:
    ticket = row[1]
    print(ticket)
    units = row[4]
    sql = "insert into reportUnits(ticket,units,indiceType) values('{}','{}','S&P')".format(ticket,units)
    cursor.execute(sql)
    db.commit()

db.close()
""" 
for row in Dowdoc.values:
    industry = row[5]
    total = total + row[3]*row[4]
    if (industry in lista)== True:
        o = lista.index(industry)
        lista.insert(o+1,lista[o+1] + row[3]*row[4])
    else:
        lista.append(industry)
        lista.append(total)

for row in Nasdaqdoc.values:
    industry = row[5]
    total2 = total2 + row[3]*row[4]
    if (industry in lista2)== True:
        o = lista2.index(industry)
        lista2.insert(o+1,lista2[o+1] + row[3]*row[4])
    else:
        lista2.append(industry)
        lista2.append(total)

for row in SyPdoc.values:
    industry = row[5]
    total3 = total3 + row[3]*row[4]
    if (industry in lista2)== True:
        o = lista3.index(industry)
        lista3.insert(o+1,lista3[o+1] + row[3]*row[4])
    else:
        lista3.append(industry)
        lista3.append(total)

print(lista) """