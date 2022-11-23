from calendar import c
import csv
import pandas
import pymongo
import time
import statistics
import mysql.connector
from cassandra.cluster import Cluster
dbtoquery = ['db25', 'db50', 'db75', 'db100']
#mock_data = pandas.read_csv("MOCK_DATA.csv");
#print(mock_data.head())

mydb1 = pymongo.MongoClient("mongodb://localhost:27017")
mydb = mydb1["v1"]
'''mycol = mydb["db100"]
query = {}
mydoc = mycol.find(query).explain()
print(mydoc)
m = mydoc['executionStats']['executionTimeMillis']
print(m)'''

'''cl = Cluster(['127.0.0.1'],port=9042)
session = cl.connect('v1')'''
'''mda = session.execute("SELECT * FROM db100",trace=True)
res = mda.get_query_trace()
#print(res)
r = str(res.duration)
r1 = r.split(':')
r = int(float(r1[0])+float(r1[1])+float(r1[2])*1000)
print(r)'''
k = []
k1=[]
k2=[]
k3=[]
v =[]
v1=[]
v2=[]
v3=[]
for o in dbtoquery:
    xk=[]
    for i in range(0,31):
        mycol = mydb[o]
        mydoc = mycol.find({}).explain()
        m = mydoc['executionStats']['executionTimeMillis']
        xk.append(m)
    k.append(xk)           
print("Mongo Q1:")        
print(k)

for o in dbtoquery:
    xk=[]
    for i in range(0,31):
        mycol = mydb[o]
        mydoc = mycol.find({'cell_site':50}).explain()
        m = mydoc['executionStats']['executionTimeMillis']
        xk.append(m)
    k1.append(xk)           
print("Mongo Q2:")        
print(k1)

for o in dbtoquery:
    xk=[]
    for i in range(0,31):
        mycol = mydb[o]
        mydoc = mycol.find({'city':'Seattle','duration':{'$gte':4000}}).explain()
        m = mydoc['executionStats']['executionTimeMillis']
        xk.append(m)
    k2.append(xk)           
print("Mongo Q3:")        
print(k2)

for o in dbtoquery:
    xk=[]
    for i in range(0,31):
        mycol = mydb[o]
        mydoc = mycol.find({'state':'New York','duration':{'$lt':2000}},{'calling_nbr':1,'called_nbr':1,'first_name':1,'_id':0}).explain()
        m = mydoc['executionStats']['executionTimeMillis']
        xk.append(m)
    k3.append(xk)           
print("Mongo Q4:")        
print(k3)



cl = Cluster(['127.0.0.1'],port=9042)
session = cl.connect('v1')

for o in dbtoquery:
    xk = []
    for i in range(0,31):
        mda = session.execute("SELECT * FROM " +o, trace=True)
        res = mda.get_query_trace()
        r = str(res.duration)
        r1 = r.split(':')
        r = int(float(r1[0]) + float(r1[1]) + float(r1[2]) * 1000)
        xk.append(r)
    v.append(xk)               
print("Cassandra Q1:")
print(v) 

for o in dbtoquery:
    xk = []
    for i in range(0,31):
        mda = session.execute("SELECT * FROM " +o+ " where cell_site=50 allow filtering", trace=True)
        res = mda.get_query_trace()
        r = str(res.duration)
        r1 = r.split(':')
        r = int(float(r1[0]) + float(r1[1]) + float(r1[2]) * 1000)
        xk.append(r)
    v1.append(xk)               
print("Cassandra Q2:")
print(v1) 

for o in dbtoquery:
    xk = []
    for i in range(0,31):
        mda = session.execute("SELECT * FROM " +o+ " where city='Seattle' and duration >= 4000 allow filtering", trace=True)
        res = mda.get_query_trace()
        r = str(res.duration)
        r1 = r.split(':')
        r = int(float(r1[0]) + float(r1[1]) + float(r1[2]) * 1000)
        xk.append(r)
    v2.append(xk)               
print("Cassandra Q3:")
print(v2) 

for o in dbtoquery:
    xk = []
    for i in range(0,31):
        mda = session.execute("SELECT first_name, calling_nbr, called_nbr FROM " +o+ " where state = 'New York' and duration < 2000 allow filtering", trace=True)
        res = mda.get_query_trace()
        r = str(res.duration)
        r1 = r.split(':')
        r = int(float(r1[0]) + float(r1[1]) + float(r1[2]) * 1000)
        xk.append(r)
    v3.append(xk)               
print("Cassandra Q4:")
print(v3) 

dbs = ['db25','db50','db75','db100']
b = []
b1 = []
b2 = []
b3 = []
bx = []
bx1 = []
bx2 = []
bx3 = []
conn = mysql.connector.connect(user='root',password='12345',port=3307,database='projdb')
curr = conn.cursor()
sql_num = 0
for j in dbs:
    curr.execute("set profiling = 1")
    for i in range(0,31):
        curr.execute("select * from "+j)
        curr.fetchall()
        curr.execute("show profiles;")
        cm = curr.fetchall()
        if sql_num < 14:
            bx.append(cm[i][1])
        elif sql_num == 14:
            bx.append(cm[14][1])
        else:
            bx.append(cm[14][1])
        sql_num += 1
    for i in range(0,31):
        curr.execute("select * from "+j+" where cell_site = 50")
        curr.fetchall()
        curr.execute("show profiles;")
        cm = curr.fetchall()
        bx1.append(cm[14][1])
        sql_num += 1
    for i in range(0,31):
        curr.execute("select * from "+j+" where city = 'Seattle' and duration >= 4000")
        curr.fetchall()
        curr.execute("show profiles;")
        cm = curr.fetchall()       
        #print(cm)
        bx2.append(cm[14][1])
        sql_num += 1
    for i in range(0,31):
        curr.execute("select first_name, calling_nbr, called_nbr from "+j+" where state = 'New York' and duration < 2000")
        curr.fetchall()
        curr.execute("show profiles;")        
        cm = curr.fetchall()
        bx3.append(cm[14][1])
        sql_num += 1
        curr.execute("set profiling = 0")
curr.close()
funnum = 0
for i in range(4):
    ac = []
    ac1 = []
    ac2 = []
    ac3 = []
    for j in range(31):
        ac.append(int(bx[funnum]*1000))
        ac1.append(int(bx1[funnum]*1000))
        ac2.append(int(bx2[funnum]*1000))
        ac3.append(int(bx3[funnum]*1000))
        funnum += 1
    b.append(ac)
    b1.append(ac1)
    b2.append(ac2)
    b3.append(ac3)
print("Mariadb Q1:")    
print(b)
print("Mariadb Q2:") 
print(b1)
print("Mariadb Q3:") 
print(b2)
print("Mariadb Q4:") 
print(b3)

with open('doc2.csv','w',newline='') as file:
    mywrite = csv.writer(file, delimiter=',')
    mywrite.writerows(k)
    mywrite.writerows(k1)
    mywrite.writerows(k2)
    mywrite.writerows(k3)
    mywrite.writerows(v)
    mywrite.writerows(v1)
    mywrite.writerows(v2)
    mywrite.writerows(v3)
    mywrite.writerows(b)
    mywrite.writerows(b1)
    mywrite.writerows(b2)
    mywrite.writerows(b3)

    
    
    
    




