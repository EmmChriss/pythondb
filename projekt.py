from tkinter import *
from tkinter import filedialog
import json
import os
import shutil

from matplotlib.font_manager import win32FontDirectory

def main():
    def create_folder():
        source_path = filedialog.askdirectory(title='Select Title')
        path = os.path.join(source_path,input_variable.get())
        os.makedirs(path)
        txtfld.delete(0,'end')
        ab_neve.set(input_variable.get()+" ")

    def drop_folder():
        source_path = filedialog.askdirectory(title='Select Title')
        shutil.rmtree(source_path)

    def create_table():
        source_path = filedialog.askdirectory(title='Select Title')
        tabla_nev.set(source_path+"\\"+tabla_nev.get()+".json")
        open(tabla_nev.get(),'w+').close()
        txtfld_tabla.delete(0,'end')

    def create_column():
        source_path = filedialog.askopenfilename(title='Select Title')
        
        f=open(source_path, "w")
        txtfld_oszlop.delete(0,'end')

    def drop_table():
        source_path = filedialog.askdirectory(title='Select Title')
        os.remove(source_path)
        txtfld_dropt.delete(0,'end')


    window = Tk()

    lbl = Label(window,text="Adatbazis bekerese", fg='red', font=('Times New Roman', 16))
    lbl.place(x=120,y=20)

    input_variable=StringVar()
    txtfld = Entry(window, text="Irja be az adatbazis nevet", bd=5, textvariable=input_variable)
    txtfld.place(x=140,y=60)

    ab_neve=StringVar()
    btn = Button(window, text="Create database", fg='green', font=("Times New Roman", 12), command=create_folder)
    btn.place(x=150,y=100)

    lbl_drop=Label(window,text="Melyik adatbazist szeretned torolni?", fg='red', font=('Times New Roman', 16))
    lbl_drop.place(x=350,y=20)

    btn_drop=Button(window,text="Drop database", fg='green', font=("Times New Roman", 12), command=drop_folder)
    btn_drop.place(x=450,y=80) 

    lbl_tabla = Label(window,text="Tabla neve:", fg='red', font=('Times New Roman', 16))
    lbl_tabla.place(x=150,y=160)

    tabla_nev=StringVar()
    txtfld_tabla = Entry(window, text="Irja be a tabla nevet", bd=5, textvariable=tabla_nev)
    txtfld_tabla.place(x=140,y=200)

    btn_tabla = Button(window, text="Create table", fg='green', font=("Times New Roman", 12), command=create_table)
    btn_tabla.place(x=160,y=240)

    lbl_oszlop = Label(window,text="Tabla tartalma:", fg='red', font=('Times New Roman', 16))
    lbl_oszlop.place(x=290,y=300)

    lbl_oszlopnev = Label(window,text="Oszlop neve:", fg='red', font=('Times New Roman', 16))
    lbl_oszlopnev.place(x=150,y=340)

    oszlop_nev=StringVar()
    txtfld_oszlop = Entry(window, text="Irja be a tabla tartalmat", bd=5, textvariable=oszlop_nev)
    txtfld_oszlop.place(x=140,y=380)

    lbl_oszloptipus = Label(window,text="Oszlop tipusa:", fg='red', font=('Times New Roman', 16))
    lbl_oszloptipus.place(x=300,y=340)

    oszlop_tipus=StringVar()
    txtfld_oszloptipus = Entry(window, text="Irja be az oszlop tipusat", bd=5, textvariable=oszlop_tipus)
    txtfld_oszloptipus.place(x=298,y=380)

    lbl_oszlopindex = Label(window,text="Oszlop indexe:", fg='red', font=('Times New Roman', 16))
    lbl_oszlopindex.place(x=450,y=340)

    oszlop_index=StringVar()
    txtfld_oszlopindex = Entry(window, text="Irja be az oszlop indexet", bd=5, textvariable=oszlop_index)
    txtfld_oszlopindex.place(x=450,y=380)

    btn_oszlop = Button(window, text="Create content", fg='green', font=("Times New Roman", 12), command=create_column)
    btn_oszlop.place(x=310,y=420)

    lbl_dropt = Label(window,text="Melyik tablat toroljuk?", fg='red', font=('Times New Roman', 16))
    lbl_dropt.place(x=400,y=160)

    droptabla_nev=StringVar()
    txtfld_dropt = Entry(window, text="Melyik tablat toroljuk?", bd=5, textvariable=droptabla_nev)
    txtfld_dropt.place(x=430,y=200)

    btn_dropt = Button(window, text="Drop table", fg='green', font=("Times New Roman", 12), command=drop_table)
    btn_dropt.place(x=455,y=240)

    window.title('Projekt')
    window.geometry("800x600+10+20")
    window.mainloop()
    
if __name__ == "__main__":
    main()