from tkinter import *
from tkinter import filedialog
import json
import os
import shutil
from PIL import ImageTk, Image
import socket
import sys

def main():
    def create_folder():
        source_path = filedialog.askdirectory(title='Select Title')
        path = os.path.join(source_path,input_variable)
        os.makedirs(path)

    def drop_folder():
        source_path = filedialog.askdirectory(title='Select Title')
        shutil.rmtree(source_path)

    def create_table():
        source_path = filedialog.askdirectory(title='Select Title')
        tabla=source_path+"\\"+tabla_nev+".json"
        f=open(tabla,'a')
        f.write("[\n]")
        f.close()

    def create_column():
        source_path = filedialog.askopenfilename(title='Select Title')
        f=open(source_path,'a+')
        size=f.tell()
        f.truncate(size-1)
        f.seek(0, 2)
        f.seek(f.tell() - 3, 0)
        karakter = f.read()
        
        if karakter[0] != '[':
            f.write(",")
            f.write("\n")
        
        data={"column name":oszlop_nev,"column type":oszlop_tipus,"column role":oszlop_index}
        json.dump(data,f,indent=4)
        f.write("\n]")
        f.close()

    def drop_table():
        source_path = filedialog.askopenfilename(title='Select Title')
        os.remove(source_path)

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


if __name__ == "__main__":
    main()