from Inputs import PiUber
import os
import sys
from tkinter import *
from tkinter import Tk
from tkinter import Button
from tkinter import ttk
from tkinter import Label
from tkinter import W
import webbrowser
import json
import logging
import ctypes
import subprocess

header_path = os.getcwd();

dir = os.path.join("Outputs")
if not os.path.exists(dir):
    os.mkdir(dir)

dir = os.path.join("generated_files")
if not os.path.exists(dir):
    os.mkdir(dir)

def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    base_path = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_path, relative_path)


def trigger_exception(message, title="EXCEPTION", uType="MB_ICONERROR", e=None):
    if e:
        try:
            logger
        except:
            logger = logging.getLogger(__name__)
        logger.exception(e)
    uTypes = {
            0: 0,
            "MB_ICONEXCLAMATION": 0x30,
            "MB_ICONWARNING": 0x30,
            "MB_ICONINFORMATION": 0x40,
            "MB_ICONASTERISK": 0x40,
            "MB_ICONQUESTION": 0x20,
            "MB_ICONSTOP": 0x10,
            "MB_ICONERROR": 0x10,
            "MB_ICONHAND": 0x10,
            }
    ctypes.windll.user32.MessageBoxW(0, message, title, uTypes[uType])

    
def slice_tracker():    
    site = variable.get();
    print('site: ', site)
    sql_orig = resource_path("Inputs\\sql_files\\slice_pull.txt");
    sql_new = header_path+"\\generated_files\\slice_pull.txt";
    slice_raw_sql = header_path + "\\Outputs\\slice_raw_pull.csv";

    wfr_coord = json.dumps(w_coords.get().replace(" ", "").split(",")).strip("[]").replace('"',"'")
    print('Wafer X Y: ', wfr_coord)

    operation_list = json.dumps(operation.get().replace(" ", "").split(",")).strip("[]").replace('"',"'")
    print('Operations: ', operation_list)

    eng_IDs_list = json.dumps(engid.get().replace(" ", "").split(",")).strip("[]").replace('"',"'")
    print('EngIDs/LotIDs: ', eng_IDs_list)

    with open(sql_orig, 'r') as file : # Read in the default sql script
        uber_script = file.read()

    # edit SQL

    uber_script = uber_script.replace('##ENG_IDS##', eng_IDs_list)# Replace the operations
    uber_script = uber_script.replace('##OPERATIONS##', operation_list )# Replace the eng_id_list

    if wfr_coord != "''":
        print("wfr_coord")
        print(wfr_coord)
        uber_script = uber_script.replace("##WHERE W_X_Y##", f"WHERE Combo_W_X_Y In ({wfr_coord})")
    else:
        uber_script = uber_script.replace('##WHERE W_X_Y##', "")
        

    with open(sql_new, 'w') as file: # Write the file out again
        file.write(uber_script)
    ## run sql script
    conn = PiUber.connect(datasource=("%s_PROD_ARIES" % site)); ## Selecting correct site
    curr = conn.cursor();
    curr.execute(uber_script);
    try:
        curr.to_csv(slice_raw_sql);
    except Exception as e:
        print(e)
        trigger_exception(f"Unable to write to file: {slice_raw_sql}\n Do you have it open in Excel?", e=e)
        return
    try:
        subprocess.Popen(slice_raw_sql, shell=True)
        print("Slice sql pull created and being opened");
    except Exception as e:
        print(e)
        trigger_exception(f"Unable to open {slice_raw_sql} in Excel, contact Idriss or Harry for help", e=e)
        return


### Main Root
root = Tk()
root.title('SliceTrackGeni v1.10')


mainframe = ttk.Frame(root, padding="60 50 60 50")
mainframe.grid(column=0, row=0, sticky=('news'))
mainframe.columnconfigure(0, weight=3)
mainframe.rowconfigure(0, weight=3)

def callback(url):
    webbrowser.open_new(url)

link1 = Label(mainframe, text="Wiki: https://goto/slicetrackpuller", fg="blue", cursor="hand2")
link1.grid(row = 0,column = 0, sticky=W, columnspan = 2)
link1.bind("<Button-1>", lambda e: callback("https://gitlab.devtools.intel.com/ianimash/slicetrackpuller/-/wikis/SliceTrackPuller"))

link2 = Label(mainframe, text="IT support contact: conor.p.boland@intel.com or idriss.animashaun@intel.com", fg="blue", cursor="hand2")
link2.grid(row = 1,column = 0, sticky=W, columnspan = 2)
link2.bind("<Button-1>", lambda e: callback("https://outlook.com"))

label_2 = Label(mainframe, text = 'Select Site: ', bg  ='black', fg = 'white')
label_2.grid(row = 1, column = 2, sticky=E)
variable = StringVar(mainframe)
variable.set("F28") # default value

sel_prod = OptionMenu(mainframe, variable, "F28", "D1D", "D1C", "F32", "F24")
sel_prod.grid(row = 2, column = 2, sticky=W)

label_0 = Label(mainframe, text = 'Enter List of EngIDs/LotIDs: ', bg  ='black', fg = 'white')
label_0.grid(row = 2, sticky=E)
engid = Entry(mainframe, width=40, relief = FLAT)
engid.insert(4,'N13603801,N13601111')
engid.grid(row = 2, column = 1, sticky=W)

label_1 = Label(mainframe, text = 'Enter List of Operations: ', bg  ='black', fg = 'white')
label_1.grid(row = 3, sticky=E)
operation = Entry(mainframe, width=40, relief = FLAT)
operation.insert(4,'119325')
operation.grid(row = 3, column = 1, sticky=W)

label_2 = Label(mainframe, text = 'Enter List of Wafer_X_Y Coordinates: ', bg  ='black', fg = 'white')
label_2.grid(row = 4, sticky=E)
w_coords = Entry(mainframe, width=40, relief = FLAT)
w_coords.insert(4,'215_6_2,216_-4_0')
w_coords.grid(row = 4, column = 1, sticky=W)

button_0 = Button(mainframe, text="Pull Slice Tracker Result", height = 1, width = 20, command = slice_tracker, bg = 'green', fg = 'white', font = '-family "SF Espresso Shack" -size 12')
button_0.grid(row = 5, column = 0, rowspan = 2, columnspan=2)

### Main loop
root.mainloop()
