# pylint: disable=C0103,C0114,C0115,C0116
import os
import tkinter as tk
from tkinter import filedialog
from src.entities.permissions.admin import Admin

admin = Admin()

if admin.isAdmin():
    folder = "C:\\Program Files\\OrFy"

    exists = os.path.exists(folder)

    if not exists:
        os.makedirs(folder)

    root = tk.Tk()
    root.withdraw()

    path_file = filedialog.askopenfilenames()

    print(path_file)
else:
    admin.runAsAdmin()
