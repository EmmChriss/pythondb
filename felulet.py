


from tkinter import *
from tkinter import filedialog
import json
import os


def main():
    def create_folder():
        source_path = filedialog.askdirectory(title='Select Title')
        path = os.path.join(source_path,input_variable.get())
        os.makedirs(path)
        create_table_window(path)

    def drop_folder():
        source_path = filedialog.askdirectory(title='Select Title')
        path = os.path.join(source_path,input_variable.get())
        os.rmdir(path)

    def create_table_window(path):
        window.destroy()
        window2=Tk()

        def create_json():
            json_file.set(path+"\\"+json_file.get()+".json")
            open(json_file.get(),'w+').close()

            window2.destroy()
            window3 = Tk()

            def add_collumn():
                collumn_name = StringVar()
                txtfld = Entry(window3, text="Irja be az oszlop nevet", bd=5, textvariable=collumn_name)
                txtfld.place(x=150,y=200)

                def convert_to_json():
                    file=open(json_file.get())
                    items=[]
                    for line in file:
                        if not line.strip():
                            continue
                        d={}
                        data=line.split('|')
                        for val in data:
                            key,sep,value=val.partition(':')
                            d[key.strip()]=value.strip()
                        items.append(d)
                    with open(json_file.get(),'w') as json_f:
                        json.dump(items,json_f)

                def write_to_json():
                    f=open(json_file.get(), "a")
                    f.write(collumn_name.get()+"\n")
                    f.close()
                    convert_to_json()
                
                btn_add = Button(window3, text="Add", fg='red', font=("Times New Roman", 12), command = write_to_json)
                btn_add.place(x=150,y=250)

            btn = Button(window3, text="Add collumn", fg='red', font=("Times New Roman", 12), command = add_collumn)
            btn.place(x=150,y=150)

            #btn_back=Button(window3, text="Add collumn", fg='red', font=("Times New Roman", 12), command = create_table_window(path))
            #btn_back.place(x=1)

            window3.title('Projekt')
            window3.geometry("400x500+10+20")
            window3.mainloop()

        lbl = Label(window2,text="Tabla letrehozasa", fg='red', font=('Times New Roman', 16))
        lbl.place(x=120,y=40)

        json_file=StringVar()
        txtfld = Entry(window2, text="Irja be a tabla nevet", bd=5, textvariable=json_file)
        txtfld.place(x=140,y=100)

        btn = Button(window2, text="Create table", fg='red', font=("Times New Roman", 12), command = create_json)
        btn.place(x=150,y=150)

        window2.title('Projekt')
        window2.geometry("400x500+10+20")
        window2.mainloop()

    window = Tk()

    lbl = Label(window,text="Adatbazis bekerese", fg='red', font=('Times New Roman', 16))
    lbl.place(x=120,y=40)

    input_variable=StringVar()
    txtfld = Entry(window, text="Irja be az adatbazis nevet", bd=5, textvariable=input_variable)
    txtfld.place(x=140,y=100)

    btn = Button(window, text="Create database", fg='red', font=("Times New Roman", 12), command=create_folder)
    btn.place(x=150,y=150)

    btn_drop=Button(window,text="Drop database", fg='red', font=("Times New Roman", 12), command=drop_folder)
    btn_drop.place(x=155,y=250)

    lbl_valasztas=Label(window,text="Melyik meglevo adatbazissal szeretne dolgozni?", fg='red', font=('Times New Roman', 16))
    lbl_valasztas.place(x=120,y=300)

    window.title('Projekt')
    window.geometry("400x500+10+20")
    window.mainloop()

if __name__ == "__main__":
    main()