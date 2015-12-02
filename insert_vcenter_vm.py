# encoding: utf-8
#!/usr/bin/python

import MySQLdb

HOST = "127.0.0.1"
USER = "root"
PASSWORD = "123qweP"
DATABASE = "nova"


def connectSQL(host=HOST, user=USER, password=PASSWORD, database=DATABASE):
    db = MySQLdb.connect(host, user, password, database)
    cursor = db.cursor()

    return cursor,db


def closeSQL(cursor, db):
    cursor.close()
    db.close()


def writeData(sqlCur, db, dataDict):
    if sqlCur is None:
        return
    if dataDict["data"] is None:
        return

    sqlCmd = "insert into %s (%s) values (%s);"
    

    dataList = dataDict["data"]

    for i in dataList: 
        itemList = ""
        valueList = ""
        for item in i.keys():
            itemList = itemList + "," + item
            if isinstance(i[item], str) or i[item] == "":
                valueList = valueList + "," + "\'" + i[item] + "\'"
            else:
                valueList = valueList + "," + str(i[item])

        try:
            sqlCur.execute(sqlCmd%(dataDict["tableName"], itemList[1:], valueList[1:]))
        except:
            print "ERROR:  cannot insert data to table" 
            print "SQL : " + sqlCmd%(dataDict["tableName"], itemList[1:], valueList[1:])

        db.commit()


