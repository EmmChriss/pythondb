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
        sock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address=('localhost', 1000)
        sock.connect(server_address)
        input_variable.set('create_folder ' + input_variable.get())
        sock.sendall(input_variable.get().encode('utf-8'))
        txtfld.delete(0,'end')
        sock.close()

    def drop_folder():
        sock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address=('localhost', 1000)
        sock.connect(server_address)
        sock.sendall('drop_folder'.encode('utf-8'))
        sock.close()

    def create_table():
        sock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address=('localhost', 1000)
        sock.connect(server_address)
        tabla_nev.set('create_table ' + tabla_nev.get())
        sock.sendall(tabla_nev.get().encode('utf-8'))
        txtfld_tabla.delete(0,'end')
        sock.close()

    def create_column():
        sock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address=('localhost', 1000)
        sock.connect(server_address)
        oszlop=StringVar()
        oszlop.set('create_column ' + oszlop_nev.get() + ' ' + oszlop_tipus.get() + ' ' + oszlop_index.get())
        sock.sendall(oszlop.get().encode('utf-8'))
        txtfld_oszlop.delete(0,'end')
        sock.close()

    def drop_table():
        sock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address=('localhost', 1000)
        sock.connect(server_address)
        sock.sendall('drop_table'.encode('utf-8'))
        sock.close()

    window = Tk()

    img = ImageTk.PhotoImage(Image.open("kep1.png"))
    canv = Label(window, image = img)
    canv.place(x=0,y=0)
    
    lbl = Label(window,text="Adatbazis bekerese", fg='red', font=('Times New Roman', 16), bg="lightblue")
    lbl.place(x=120,y=20)

    input_variable=StringVar()
    txtfld = Entry(window, text="Irja be az adatbazis nevet", bd=5, textvariable=input_variable)
    txtfld.place(x=140,y=60)

    ab_neve=StringVar()
    btn = Button(window, text="Create database", fg='green', font=("Times New Roman", 12), command=create_folder, bg="lightblue")
    btn.place(x=150,y=100)

    lbl_drop=Label(window,text="Melyik adatbazist szeretned torolni?", fg='red', font=('Times New Roman', 16), bg="lightblue")
    lbl_drop.place(x=350,y=20)

    btn_drop=Button(window,text="Drop database", fg='green', font=("Times New Roman", 12), command=drop_folder, bg="lightblue")
    btn_drop.place(x=450,y=80) 

    lbl_tabla = Label(window,text="Tabla neve:", fg='red', font=('Times New Roman', 16), bg="lightblue")
    lbl_tabla.place(x=150,y=160)

    tabla_nev=StringVar()
    txtfld_tabla = Entry(window, text="Irja be a tabla nevet", bd=5, textvariable=tabla_nev)
    txtfld_tabla.place(x=140,y=200)

    btn_tabla = Button(window, text="Create table", fg='green', font=("Times New Roman", 12), command=create_table, bg="lightblue")
    btn_tabla.place(x=160,y=240)

    lbl_oszlop = Label(window,text="Tabla tartalma:", fg='red', font=('Times New Roman', 16), bg="lightblue")
    lbl_oszlop.place(x=290,y=300)

    lbl_oszlopnev = Label(window,text="Oszlop neve:", fg='red', font=('Times New Roman', 16), bg="lightblue")
    lbl_oszlopnev.place(x=150,y=340)

    oszlop_nev=StringVar()
    txtfld_oszlop = Entry(window, text="Irja be a tabla tartalmat", bd=5, textvariable=oszlop_nev)
    txtfld_oszlop.place(x=140,y=380)

    lbl_oszloptipus = Label(window,text="Oszlop tipusa:", fg='red', font=('Times New Roman', 16), bg="lightblue")
    lbl_oszloptipus.place(x=300,y=340)

    tipusok=['int','string','date','datetime','float','bit']
    oszlop_tipus=StringVar()
    option_menu=OptionMenu(window,oszlop_tipus,*tipusok)
    option_menu.place(x=298,y=380)

    lbl_oszlopindex = Label(window,text="Oszlop indexe:", fg='red', font=('Times New Roman', 16), bg="lightblue")
    lbl_oszlopindex.place(x=450,y=340)

    oszlop_index=StringVar()
    indexek=['primary-key', 'foreign-key', 'unique', 'index', 'none']
    option_menu_index=OptionMenu(window, oszlop_index, *indexek)
    option_menu_index.place(x=450,y=380)

    btn_oszlop = Button(window, text="Create content", fg='green', font=("Times New Roman", 12), command=create_column, bg="lightblue")
    btn_oszlop.place(x=310,y=420)

    lbl_dropt = Label(window,text="Melyik tablat toroljuk?", fg='red', font=('Times New Roman', 16), bg="lightblue")
    lbl_dropt.place(x=400,y=160)

    btn_dropt = Button(window, text="Drop table", fg='green', font=("Times New Roman", 12), command=drop_table, bg="lightblue")
    btn_dropt.place(x=455,y=210)

    window.title('Projekt')
    window.geometry("800x600+10+20")
    window.mainloop()

    
if __name__ == "__main__":
    main()