# pylint: disable=C0103,C0114,C0115,C0116
import os
from tkinter import filedialog
import sys
#import sqlalchemy
import pandas as pd
import mariadb
from entities.admin import Admin

# Instancia o módulo admin para iniciar o sistema como administrador
admin = Admin()

if admin.isAdmin():

    print("Boas-vindas a OrFy!")

    folder = "C:\\Program Files\\OrFy"

    exists = os.path.exists(folder)

    if not exists:
        # Cria um diretório na pasta do sistema Program Files
        os.makedirs(folder)

    file_name = filedialog.askopenfilenames()    # Resulta em um tuple
    # Converter tuple em str para consulta do endswith()
    file_name = ''.join(file_name)

    if file_name.endswith('.csv'):
        # Conexão com o banco de dados no mariadb
        try:
            conn = mariadb.connect(
                user="root",
                password="1234",
                port=3306,
                database="aror")

            # Iniciando cursor para manipulação do banco
            cursor = conn.cursor()

            # Queries para criar tabelas no banco se não existirem
            create_query1 = "CREATE TABLE IF NOT EXISTS medicamento (IDMEDICAMENTO INT PRIMARY KEY AUTO_INCREMENT, PRINCIPIO_ATIVO VARCHAR(500), CLASSE_TERAPEUTICA VARCHAR(50), NOME VARCHAR(50));"
            create_query2 = "CREATE TABLE IF NOT EXISTS venda (IDVENDA INT PRIMARY KEY AUTO_INCREMENT, QUANTIDADE INT, IDMEDICAMENTO INT, FOREIGN KEY(IDMEDICAMENTO) REFERENCES MEDICAMENTO(IDMEDICAMENTO));"
            create_query3 = "CREATE TABLE IF NOT EXISTS consulta (IDCONSULTA INT PRIMARY KEY AUTO_INCREMENT, RESULTADO BIGINT(15));"
            create_query4 = "CREATE TABLE IF NOT EXISTS filtro (IDFILTRO INT PRIMARY KEY AUTO_INCREMENT, TIPO VARCHAR(18), CONTEUDO VARCHAR(500), IDCONSULTA INT,	FOREIGN KEY(IDCONSULTA)	REFERENCES CONSULTA(IDCONSULTA));"
            create_query5 = "CREATE TABLE IF NOT EXISTS periodo (IDPERIODO INT PRIMARY KEY AUTO_INCREMENT, PERIODO_INICIAL DATE, PERIODO_FINAL DATE, IDCONSULTA INT, FOREIGN KEY(IDCONSULTA) REFERENCES CONSULTA(IDCONSULTA));"

            cursor.execute(create_query1)
            cursor.execute(create_query2)
            cursor.execute(create_query3)
            cursor.execute(create_query4)
            cursor.execute(create_query5)

            # Cria um dataframe baseado no arquivo escolhido pelo usuário
            df_venda = pd.read_csv(file_name, delimiter = ";", quotechar = '"', quoting = (3), encoding = 'utf-8')

            df_venda.to_sql('venda', con = conn, if_exists = 'replace')


        except mariadb.Error as e:
            print(f"Error connecting to MariaDB Platform: {e}")
            sys.exit(1)
    else:
        print('Arquivo não suportado.')

else:
    admin.runAsAdmin()
