# pylint: disable=C0103,C0114,C0115,C0116, C0209, C0301, W0105
import os
from tkinter import filedialog
import sys
from sqlalchemy import create_engine
import pandas as pd
import mariadb

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
        df_venda = pd.read_csv(file_name, delimiter = ";", quotechar = '"', quoting = (3), encoding = 'utf-8', encoding_errors = 'ignore')

        engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="root",
                               pw="1234",
                               db="aror"))

        df_venda.to_sql('venda', con = engine, if_exists = 'replace')


        """
        #  table_consulta = Acesso a lista da tabela de consultas fora do loop principal
        #  table_medicamento = Acesso a lista da tabela de medicamentos fora do loop principal
        #  table_filtro = Acesso a lista da tabela de filtros fora do loop principal
        #  table_periodo = Acesso a lista da tabela de períodos fora do loop principal
        #  table_venda = Acesso em loop para escolher a table_venda[i] através do período selecionado
        #  para mesclar num só dataset e resultar nessa variável

        table_venda_nome = input("Qual o nome da tabela de vendas?" )

        table_medicamento_nome = input("Qual o nome da tabela de medicamentos? ")

        qtd_filtros = input("Quantos filtros quer?")

        filtros_extra = ""

        for (i = 0; i < qtd_filtros; i++):
            tipo_filtro[i] = input(f"Qual o tipo do filtro {i}? ")

            conteudo_filtro[i] = input(f"Qual o conteúdo do filtro {i}? ")

            filtro[i] = "{table2}.{tipo_filtro[i]} LIKE '{conteudo_filtro[i]}% OR'"

            filtros_extra += filtro[i]

        filtros_extra.pop()
        filtros_extra.pop()
        
        for consulta in table_consulta:
        for (i = 0; i < table_consulta.length; i++):

        if consulta.id_consulta ==

            view_nome = input("Como gostaria de nomear a visualização? ")

            view_consulta[i] = f"CREATE VIEW {view_nome} AS SELECT ANO_VENDA, SUM(QTD_VENDIDA) FROM {table_venda_nome} INNER JOIN {table_medicamento_nome} ON {table1}.PRINCIPIO_ATIVO={table2}.PRINCIPIO_ATIVO WHERE {table2}.{tipo_filtro} LIKE '{conteudo_filtro}%' {filtros_extra} AND {table1}.PRINCIPIO_ATIVO IS NOT NULL AND {table2}.PRINCIPIO_ATIVO IS NOT NULL"

        """


    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)
else:
    print('Arquivo não suportado.')
