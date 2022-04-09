from pymongo import MongoClient
from tkinter import *
import pymongo as py

client = MongoClient('mongodb+srv://agica:agica2001@cluster0.xdmq9.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')
client2 = MongoClient('mongodb+srv://agica:agica2001@indexcluster.vkjyv.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')

def create_database(mydatabase):
    mydb = client[mydatabase]
    mydb.create_collection("database created")
    
    mydb_index = client2[mydatabase]
    mydb_index.create_collection("database created")

def drop_database(mydatabase):
    try:
        client.drop_database(mydatabase)
    except:
        error_window=Tk()
        lbl_error = Label(error_window,text="Mar torolve van az adatbazis a mongoba!", fg='red', font=('Times New Roman', 16), bg="lightblue")
        lbl_error.place(x=10,y=50)
        error_window.title('Error')
        error_window.geometry("400x100+10+20")
        error_window.lift()
        error_window.mainloop()
    
    try:
        client2.drop_database(mydatabase)
    except:
        error_window=Tk()
        lbl_error = Label(error_window,text="Mar torolve van az adatbazis a mongoba!", fg='red', font=('Times New Roman', 16), bg="lightblue")
        lbl_error.place(x=10,y=50)
        error_window.title('Error')
        error_window.geometry("400x100+10+20")
        error_window.lift()
        error_window.mainloop()

def create_table(mydatabase, mytable):
    mydb = client[mydatabase]
    if("database created" in mydb.list_collection_names()):
        mycol = mydb["database created"]
        mycol.drop()
    mydb.create_collection(mytable)
    
    mydb_index = client2[mydatabase]
    if("database created" in mydb_index.list_collection_names()):
        mycol_index = mydb_index["database created"]
        mycol_index.drop()
    mydb_index.create_collection(mytable + '.pkUnique')
    mydb_index.create_collection(mytable + '.pkNotUnique')
    mydb_index.create_collection(mytable + '.fk')
    
def drop_table(mydatabase, mytable):
    mydb = client[mydatabase]
    mycol = mydb[mytable]
    mycol.drop()

    mydb_index = client2[mydatabase]
    mycol_index = mydb_index[mytable+'.pkUnique']
    mycol_index.drop()
    mycol_index = mydb_index[mytable+'.pkNotUnique']
    mycol_index.drop()
    mycol_index = mydb_index[mytable+'.fk']
    mycol_index.drop()

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
            "values":values[2:]
            }
    try:
        rec_id1 = mycol.insert_one(emp_rec1)
    except:
        error_window=Tk()
        lbl_error = Label(error_window,text="Mar letezik az adott index", fg='red', font=('Times New Roman', 16), bg="lightblue")
        lbl_error.place(x=10,y=50)
        error_window.title('Error')
        error_window.geometry("400x100+10+20")
        error_window.lift()
        error_window.mainloop()

def delete(value, mydatabase, mytable):
    mydb = client[mydatabase]
    mycol = mydb[mytable]
    myquery={"_id": value}
    try:
        mycol.delete_one(myquery)
    except:
        error_window=Tk()
        lbl_error = Label(error_window,text="Nincs ilyen index", fg='red', font=('Times New Roman', 16), bg="lightblue")
        lbl_error.place(x=10,y=50)
        error_window.title('Error')
        error_window.geometry("400x100+10+20")
        error_window.lift()
        error_window.mainloop()

def add_column(value, mydatabase, mytable):
    mytable=mytable.split(".")
    mydb_index = client2[mydatabase]
    mycol_index_pku = mydb_index[mytable[0]+'.pkUnique']
    mycol_index_pknu = mydb_index[mytable[0]+'.pkNotUnique']
    mycol_index_fk = mydb_index[mytable[0]+'.fk']
    seged=value.split(' ')
    if(seged[1]=='primary-key-unique'):
        emp_rec1 = {
            "values":seged[0]
            }
        try:
            rec_id1 = mycol_index_pku.insert_one(emp_rec1)
        except:
            error_window=Tk()
            lbl_error = Label(error_window,text="Mar letezik az adott index", fg='red', font=('Times New Roman', 16), bg="lightblue")
            lbl_error.place(x=10,y=50)
            error_window.title('Error')
            error_window.geometry("400x100+10+20")
            error_window.lift()
            error_window.mainloop()
    elif(seged[1]=='primary-key-not-unique'):
        emp_rec1 = {
            "values":seged[0]
            }
        try:
            rec_id1 = mycol_index_pknu.insert_one(emp_rec1)
        except:
            error_window=Tk()
            lbl_error = Label(error_window,text="Mar letezik az adott index", fg='red', font=('Times New Roman', 16), bg="lightblue")
            lbl_error.place(x=10,y=50)
            error_window.title('Error')
            error_window.geometry("400x100+10+20")
            error_window.lift()
            error_window.mainloop()
    elif(seged[1]=='foreign-key'):
        reference=seged[2].split('/')
        reference_seged=reference[len(reference)-1].split('.')
        emp_rec1 = {
            "values":seged[0],
            "reference":reference_seged[0]
            }
        try:
            rec_id1 = mycol_index_fk.insert_one(emp_rec1)
        except:
            error_window=Tk()
            lbl_error = Label(error_window,text="Mar letezik az adott index", fg='red', font=('Times New Roman', 16), bg="lightblue")
            lbl_error.place(x=10,y=50)
            error_window.title('Error')
            error_window.geometry("400x100+10+20")
            error_window.lift()
            error_window.mainloop()