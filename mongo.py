from pymongo import MongoClient

client = MongoClient('mongodb+srv://agica:agica2001@cluster0.xdmq9.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')

def create_database(mydatabase):
    mydb = client[mydatabase]
    mydb.create_collection("database created")

def drop_database(mydatabase):
    client.drop_database(mydatabase)

def create_table(mydatabase, mytable):
    mydb = client[mydatabase]
    if("database created" in mydb.list_collection_names()):
        mycol = mydb["database created"]
        mycol.drop()
    mydb.create_collection(mytable)
    
def drop_table(mydatabase, mytable):
    mydb = client[mydatabase]
    mycol = mydb[mytable]
    mycol.drop()

def insert(values, mydatabase, mytable):
    mydb = client[mydatabase]
    mycol = mydb[mytable]
    if(len(values)==1):
        emp_rec1 = {
            "_id":values[0],
            "values":"NULL"
            }
    else:
        emp_rec1 = {
            "_id":values[0],
            "values":values
            }
    rec_id1 = mycol.insert_one(emp_rec1)

def delete(value, mydatabase, mytable):
    mydb = client[mydatabase]
    mycol = mydb[mytable]
    myquery={"_id": value}
    mycol.delete_one(myquery)