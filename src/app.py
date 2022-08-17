# pylint: disable=C0103,C0114,C0115,C0116, C0209, C0301, W0105
import os
from tkinter import filedialog
import sys
import pwinput as pw
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

# Configurar mecanismo de período+datasets aqui

class App:
    if file_name.endswith('.csv'):
        # Conexão com o banco de dados no mariadb
        try:
            user = input("Digite seu usuário: ")
            pw = pw.pwinput(prompt = 'Digite sua senha: ')

            port_quest = input("Gostaria de usar port padrão?(s/n) ")

            port = 3306

            if port_quest.lower() == "n":
                port = (int(input("Digite o port de sua escolha: ")))

            db = input("Digite o nome da sua database: ")

            def connection(self):
                conn = mariadb.connect(user=App.user, password=App.pw, port=App.port, database=App.db)
                return conn

            # Iniciando cursor para manipulação do banco
            connect = connection()
            cursor = connect.cursor()

            # Criando as tabelas a serem utilizadas
            table_medicamento_nome = input("Que nome quer dar à tabela de medicamentos? ")

            table_venda_nome = input("Que nome quer dar à tabela de vendas? ")

            table_consulta_nome = input("Que nome quer dar à tabela de consultas? ")

            table_filtro_nome = input("Que nome quer dar à tabela de filtros? ")

            table_periodo_nome = input("Que nome quer dar à tabela de períodos? ")

            # Queries para criar tabelas no banco se não existirem
            create_query1 = f"CREATE TABLE IF NOT EXISTS {table_medicamento_nome} (ID_MEDICAMENTO INT UNSIGNED PRIMARY KEY AUTO_INCREMENT, PRINCIPIO_ATIVO VARCHAR(380), CLASSE_TERAPEUTICA VARCHAR(101), NOME_PRODUTO VARCHAR(70));"
            create_query2 = f"CREATE TABLE IF NOT EXISTS {table_venda_nome} (ID_VENDA INT UNSIGNED PRIMARY KEY AUTO_INCREMENT, QTD_VENDIDA INT UNSIGNED, MES_VENDA VARCHAR(2), ANO_VENDA VARCHAR(4), ID_MEDICAMENTO INT UNSIGNED, FOREIGN KEY(IDMEDICAMENTO) REFERENCES MEDICAMENTO(ID_MEDICAMENTO));"
            create_query3 = f"CREATE TABLE IF NOT EXISTS {table_consulta_nome} (ID_CONSULTA INT UNSIGNED PRIMARY KEY AUTO_INCREMENT, RESULTADO INT UNSIGNED, ID_FILTRO INT UNSIGNED, ID_PERIODO INT UNSIGNED, FOREIGN KEY(ID_FILTRO) REFERENCES {table_filtro_nome}(ID_FILTRO), FOREIGN KEY(ID_PERIODO) REFERENCES {table_periodo_nome}(ID_PERIODO));"
            create_query4 = f"CREATE TABLE IF NOT EXISTS {table_filtro_nome} (ID_FILTRO INT UNSIGNED PRIMARY KEY AUTO_INCREMENT, TIPO VARCHAR(18), CONTEUDO VARCHAR(380));"
            create_query5 = f"CREATE TABLE IF NOT EXISTS {table_periodo_nome} (ID_PERIODO INT UNSIGNED PRIMARY KEY AUTO_INCREMENT, MES VARCHAR(2), ANO VARCHAR(4));"

            cursor.execute(create_query1)
            cursor.execute(create_query2)
            cursor.execute(create_query3)
            cursor.execute(create_query4)
            cursor.execute(create_query5)

            # Instanciando as tabelas criadas em variáveis
            table_consulta_query = f"SELECT * FROM {table_consulta_nome};"
            table_medicamento_query = f"SELECT * FROM {table_medicamento_nome};"
            table_filtro_query = f"SELECT * FROM {table_filtro_nome};"
            table_periodo_query = f"SELECT * FROM {table_periodo_nome};"

            table_medicamento = cursor.execute(table_medicamento_query)
            table_consulta = cursor.execute(table_consulta_query)
            table_filtro = cursor.execute(table_filtro_query)
            table_periodo = cursor.execute(table_periodo_query)

            # Criando um dataframe baseado no dataset de vendas
            df_venda = pd.read_csv(file_name, delimiter = ";", quotechar = '"', quoting = (3), encoding = 'utf-8', encoding_errors = 'ignore')

            # Conectando com o banco de dados para enviar dataframe
            engine = create_engine(f"mysql+pymysql://{user}:{pw}@localhost/{db}")

            # Enviando o dataframe de vendas para a tabela pertinente
            df_venda.to_sql(table_venda_nome, con = engine, if_exists = 'replace')

            consulta_quest = input("Gostaria de iniciar uma nova consulta?(s/n)")

            if consulta_quest == "s":
                view_filtro_nome = input("Como gostaria de nomear a view de medicamentos? ")

                # Selecionando as colunas a serem visualizadas na tabela filtrada de medicamentos
                colunas_escolhidas_visual = list(input("Quais colunas gostaria de visualizar?(CLASSE_TERAPEUTICA, PRINCIPIO_ATIVO, NOME_PRODUTO) "))
                colunas = ["CLASSE_TERAPEUTICA", "PRINCIPIO_ATIVO", "NOME_PRODUTO"]
                colunas_filtradas = len(colunas_escolhidas_visual) * []

                for i in enumerate(colunas_filtradas):
                    for coluna in colunas:
                        if coluna == colunas[i]:
                            colunas_filtradas = colunas_filtradas.append(coluna)
                        else:
                            continue
                colunas_filtradas = ", ".join(colunas_filtradas)    # Mostra as colunas que serão vistas na tabela filtrada

                qtd_filtros = (int(input("Quantos filtros quer? ")))

                counter = 1
                filtros_extra = ""
                view_filtro_list = []

                while counter <= qtd_filtros:

                    tipo_filtro = input(f"Qual o tipo do filtro {counter}? ")

                    conteudo_filtro = input(f"Qual o conteúdo do filtro {counter}? ")

                    filtro = f"{tipo_filtro},{conteudo_filtro};"

                    filtro_str = f"{table_medicamento_nome}.{tipo_filtro} LIKE '%{conteudo_filtro}%' ,"

                    filtros_extra += filtro_str

                    view_filtro_list = view_filtro_list.append(filtro)

                    counter += 1
                filtros_extra = filtros_extra.split(",")
                filtros_extra = "OR ".join(filtros_extra)   # Mostra o texto dos filtros selecionados

                tipos_filtro = []
                for filtro in view_filtro_list:
                    tipo_filtro, conteudo_filtro = filtro.split(",")
                    tipos_filtro = tipos_filtro.append(tipo_filtro) # Mostra a lista de tipos de filtros preenchida com a lista criada no loop de filtro

                order = input("Qual coluna gostaria de usar para ordenar a tabela?(NOME_PRODUTO, CLASSE_TERAPEUTICA, PRINCIPIO_ATIVO) ")
                order_by = ""
                for tipo_filtro in tipos_filtro:
                    if order == tipo_filtro:
                        order_by = order    # Mostra a coluna usada para o order by

                view_filtro_query = f"CREATE TABLE IF NOT EXISTS {view_filtro_nome} AS SELECT {colunas_filtradas} FROM {table_medicamento_nome} WHERE {filtros_extra} ORDER BY {order_by};"

                view_soma_nome = input("Como gostaria de nomear a visualização do resultado? ")

                soma_query = f"CREATE VIEW {view_soma_nome} AS SELECT ANO_VENDA, SUM(QTD_VENDIDA) FROM {table_venda_nome} INNER JOIN {view_filtro_nome} ON {view_filtro_nome}.PRINCIPIO_ATIVO={table_venda_nome}.PRINCIPIO_ATIVO WHERE {table_medicamento_nome}.{tipo_filtro} LIKE '{conteudo_filtro}%' {filtros_extra} AND {table_venda_nome}.PRINCIPIO_ATIVO IS NOT NULL AND {table_medicamento_nome}.PRINCIPIO_ATIVO IS NOT NULL;"

                cursor.execute(soma_query)

            else:
                conn = connection()
                cursor = conn.cursor()

                table_consulta_query = "SELECT * FROM consulta;"
                table_consulta = cursor.execute(table_consulta_query)

                old_consulta = input("Gostaria de acessar uma consulta anterior?(s/n) ")

                if old_consulta.lower() == "s":
                    id_consulta = input("Qual o id da consulta que você gostaria de acessar? ")

                    for consulta in table_consulta:
                        if table_consulta["ID_CONSULTA"] == id_consulta:
                            print(consulta)

        except mariadb.Error as e:
            print(f"Error connecting to MariaDB Platform: {e}")
            sys.exit(1)
    else:
        print('Arquivo não suportado.')
