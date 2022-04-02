from tkinter import *
from tkinter import filedialog
import json
import os
import shutil
from PIL import ImageTk, Image
import socket
import sys
import parser
import mongo

def main():
    def create_folder():
        source_path = filedialog.askdirectory(title='Select Title')
        path = os.path.join(source_path,input_variable)
        if(os.path.exists(path)):
            error_window=Tk()
            lbl_error = Label(error_window,text="Mar letezik ez az adatbazis!", fg='red', font=('Times New Roman', 16), bg="lightblue")
            lbl_error.place(x=10,y=50)
            error_window.title('Error')
            error_window.geometry("400x100+10+20")
            error_window.lift()
            error_window.mainloop()
            return
        os.makedirs(path)
        mongo.create_database(input_variable)

    def drop_folder():
        source_path = filedialog.askdirectory(title='Select Title')
        shutil.rmtree(source_path)
        seged=source_path.split("/")
        mongo.drop_database(seged[len(seged)-1])

    def create_table():
        source_path = filedialog.askdirectory(title='Select Title')
        tabla=source_path+"\\"+tabla_nev+".json"
        if(os.path.exists(tabla)):
            error_window=Tk()
            lbl_error = Label(error_window,text="Mar letezik ez a tabla az adatbazisban!", fg='red', font=('Times New Roman', 16), bg="lightblue")
            lbl_error.place(x=10,y=50)
            error_window.title('Error')
            error_window.geometry("400x100+10+20")
            error_window.lift()
            error_window.mainloop()
            return
        f=open(tabla,'a')
        f.write("[\n]")
        f.close()
        seged=source_path.split("/")
        mongo.create_table(seged[len(seged)-1], tabla_nev)

    def create_column():
        source_path = filedialog.askopenfilename(title='Select Title')
        f=open(source_path,'a+')

        okes=0
        if(oszlop_index=='foreign-key'):
            referencia = filedialog.askopenfilename(title='Select reference')
            while(source_path==referencia):
                error_window=Tk()
                lbl_error = Label(error_window,text="Sajat magara nem mutatahat!", fg='red', font=('Times New Roman', 16), bg="lightblue")
                lbl_error.place(x=10,y=50)
                error_window.title('Error')
                error_window.geometry("400x100+10+20")
                error_window.lift()
                error_window.mainloop()
                referencia = filedialog.askopenfilename(title='Select reference')
            with open(referencia, 'r') as g:
                data=json.load(g)
            for i in data:
                if(i['column role']=='primary-key'):
                    okes=1
                    break
                else:
                    okes=0
            if(okes==0):
                error_window=Tk()
                lbl_error = Label(error_window,text="Ebben a tablaban nincs primary key!", fg='red', font=('Times New Roman', 16), bg="lightblue")
                lbl_error.place(x=10,y=50)
                error_window.title('Error')
                error_window.geometry("400x100+10+20")
                error_window.lift()
                error_window.mainloop()
                create_column()

        size=f.tell()
        f.truncate(size-1)
        f.seek(0, 2)
        f.seek(f.tell() - 3, 0)
        karakter = f.read()
        
        if karakter[0] != '[':
            f.write(",")
            f.write("\n")

        if(oszlop_index=='foreign-key'):         
            data={"column name":oszlop_nev,"column type":oszlop_tipus,"column role":oszlop_index,"references":referencia}
        else:
            data={"column name":oszlop_nev,"column type":oszlop_tipus,"column role":oszlop_index}
        json.dump(data,f,indent=4)
        f.write("\n]")
        f.close()

    def drop_table():
        source_path = filedialog.askopenfilename(title='Select Title')
        os.remove(source_path)
        seged=source_path.split("/")
        collection_nev=seged[len(seged)-1].split(".")
        mongo.drop_table(seged[len(seged)-2], collection_nev[0])

    def insert():
        source_path = filedialog.askopenfilename(title='Select Title')
        client.send(source_path.encode('utf-8'))
    
    def delete():
        source_path=filedialog.askopenfilename(title='Select Title')
        client.send(source_path.encode('utf-8'))

    def check_insert():
        f=open(valtozok[1], 'r')
        data=json.load(f)
        seged=valtozok[2].split("#")
        if(len(data)!=len(seged)):
            error_window=Tk()
            lbl_error = Label(error_window,text="Less or more arguments are needed", fg='red', font=('Times New Roman', 16), bg="lightblue")
            lbl_error.place(x=10,y=50)
            error_window.title('Less or more arguments are needed')
            error_window.geometry("400x100+10+20")
            error_window.lift()
            error_window.mainloop()
            return
        j=0
        for i in data:
            if(parser.parser_input(seged[j], i['column type'])==False):
                error_window=Tk()
                error_message=StringVar()
                error_message.set("Input does not match! You need to put in the "+str(j+1)+" . place a variable type "+str(i['column type']))
                lbl_error = Label(error_window,text=error_message.get(), fg='red', font=('Times New Roman', 16), bg="lightblue")
                lbl_error.place(x=10,y=50)
                error_window.title('Argument does not match the type')
                error_window.geometry("400x100+10+20")
                error_window.lift()
                error_window.mainloop()
                return
            j=j+1
        seged=valtozok[1].split("/")
        tabla=seged[len(seged)-1].split(".")
        mongo.insert(valtozok[2], seged[len(seged)-2], tabla[0])

    def check_delete():
        f=open(valtozok[1], 'r')
        data=json.load(f)
        seged=valtozok[2].split("=")
        if(len(seged)<2):
            error_window=Tk()
            error_message=StringVar()
            error_message.set("Two arguments are needed")
            lbl_error = Label(error_window,text=error_message.get(), fg='red', font=('Times New Roman', 16), bg="lightblue")
            lbl_error.place(x=10,y=50)
            error_window.title('Two arguments are needed')
            error_window.geometry("400x100+10+20")
            error_window.lift()
            error_window.mainloop()
            return
        ok=0
        for i in data:
            if(i['column name']==seged[0]):
                ok=1
                if(parser.parser_input(seged[1], i['column type'])==False):
                    error_window=Tk()
                    error_message=StringVar()
                    error_message.set("Input does not match! You need to use variable type "+str(i['column type']))
                    lbl_error = Label(error_window,text=error_message.get(), fg='red', font=('Times New Roman', 16), bg="lightblue")
                    lbl_error.place(x=10,y=50)
                    error_window.title('Argument does not match the type')
                    error_window.geometry("400x100+10+20")
                    error_window.lift()
                    error_window.mainloop()
                    return
        if(ok==0):
            error_window=Tk()
            error_message=StringVar()
            error_message.set("The table does not contain a column named "+seged[0])
            lbl_error = Label(error_window,text=error_message.get(), fg='red', font=('Times New Roman', 16), bg="lightblue")
            lbl_error.place(x=10,y=50)
            error_window.title('No such column in the table')
            error_window.geometry("400x100+10+20")
            error_window.lift()
            error_window.mainloop()
            return 
        database=valtozok[1].split("/")
        table=database[len(database)-1].split(".")
        mongo.delete(seged[1], database[len(database)-2], table[0])
        

    sock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address=('localhost', 1000)
    sock.bind(server_address)
    sock.listen(5)
    while True:
        client, address=sock.accept()
        data=client.recv(2048)
        data=data.decode()
        valtozok=data.split()
        if(valtozok[0]=='create_folder'):
            input_variable = valtozok[1]
            create_folder()
        elif(valtozok[0]=='drop_folder'):
            drop_folder()
        elif(valtozok[0]=='create_table'):
            tabla_nev=valtozok[1]
            create_table()
        elif(valtozok[0]=='create_column'):
            oszlop_nev=valtozok[1]
            oszlop_tipus=valtozok[2]
            oszlop_index=valtozok[3]
            create_column()
        elif(valtozok[0]=='drop_table'):
            drop_table()
        elif(valtozok[0]=='insert'):
            insert()
        elif(valtozok[0]=='delete'):
            delete()
        elif(valtozok[0]=='check_insert'):
            check_insert()
        elif(valtozok[0]=='check_delete'):
            check_delete()


if __name__ == "__main__":
    main()