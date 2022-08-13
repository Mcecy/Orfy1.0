# pylint: disable=C0103,C0114,C0115,C0116
"""
Modulo de janelas do tkinter
"""
import tkinter as tk
from tkinter import filedialog


class FileDialog:
    """
    Classe para o filedialog
    """
    def opendialog(self):
        root = tk.Tk()
        root.mainloop()
#        root.withdraw()

        path_file = filedialog.askopenfilenames()

        return path_file

    def closedialog():
        return root.
