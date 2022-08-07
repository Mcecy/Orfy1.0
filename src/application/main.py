# pylint: disable=C0103,C0114,C0115,C0116
import os, tkinter, ctypes, sys

def isAdmin():
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except:
        return False

def runAsAdmin():
    return ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, " ".join(sys.argv), None, 1)

if isAdmin():
    folder = "C:\\Program Files\\OrFy"

    exists = os.path.exists(folder)

    if not exists:
        os.makedirs(folder)

    tkinter.filedialog.askopenfile()
else:
    runAsAdmin()
