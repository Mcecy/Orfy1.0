# pylint: disable=C0103,C0114,C0115,C0116
import ctypes
import sys

class Admin:
    def isAdmin(self):
        if ctypes.windll.shell32.IsUserAnAdmin():
            return True
        else:
            return False

    def runAsAdmin(self):
        shell32 = ctypes.windll.shell32
        return shell32.ShellExecuteW(None, "runas", sys.executable, " ".join(sys.argv), None, 1)
