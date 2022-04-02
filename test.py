from pymongo import MongoClient
client = MongoClient("mongodb+srv://agica:agica2001@cluster0.xdmq9.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")

db = client.Teszt
collection = db.vadallatok
  
emp_rec1 = {
        "name":"Mr.Geek",
        "eid":25,
        "location":"delhi"
        }
emp_rec2 = {
        "name":"Mr.Shaurya",
        "eid":1,
        "location":"delhi"
        }
  
# Insert Data
rec_id1 = collection.insert_one(emp_rec1)
rec_id2 = collection.insert_one(emp_rec2)
  
print("Data inserted with record ids",rec_id1," ",rec_id2)
  
# Printing the data inserted
cursor = collection.find()
for record in cursor:
    print(record)