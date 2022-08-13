# pylint: disable=C0103,C0114,C0115,C0116
import os
from tkinter import filedialog
import pandas as pd
import mariadb
import sys
from entities.admin import Admin

admin = Admin()

if admin.isAdmin():
    folder = "C:\\Program Files\\OrFy"

    exists = os.path.exists(folder)

    if not exists:
        os.makedirs(folder)

    file_name = filedialog.askopenfilenames()

    print(file_name)

    if file_name.endswith('.csv'):
        # Connect to MariaDB Platform
        try:
            conn = mariadb.connect(
                user="root@localhost",
                password="1234",
                port=3306,
                database="aror"

            )
        except mariadb.Error as e:
            print(f"Error connecting to MariaDB Platform: {e}")
            sys.exit(1)

        # Get Cursor
        cur = conn.cursor()

        df = pd.read_csv(file_name)

        cur.execute(
            "INSERT INTO aror;",
            (df,))

    else:
        print('Arquivo não suportado.')

else:
    admin.runAsAdmin()
