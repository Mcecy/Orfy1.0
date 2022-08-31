# pylint: disable=C0103,C0114,C0115,C0116, C0209, C0301, W0105
import os
import sys
import pwinput as pw
from sqlalchemy import create_engine
import pandas as pd
import mariadb
from retrying import retry
class App:

    print("Boas-vindas a OrFy!")

    # Cria um diretório na pasta do sistema Program Files
    folder = "C:\\Program Files\\OrFy"

    exists = os.path.exists(folder)

    if not exists:
        os.makedirs(folder)

    med_file = 'src\\entities\\datasets\\DADOS_ABERTOS_MEDICAMENTOS.csv'

    while True:
        # Conexão com o banco de dados no mariadb
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

        try:
            # Iniciando cursor para manipulação do banco
            connect = connection()
            cursor = connect.cursor()
        except mariadb.Error as e:
            print(f"Erro ao conectar com o MariaDB: {e}")
            sys.exit(1)

        # Criando as tabelas a serem utilizadas
        table_quest = input("Gostaria de criar novas tabelas?(s/n)")

        if table_quest.lower() == 's':
            table_medicamento_nome = input("Que nome quer dar à tabela de medicamentos? ")

            table_venda_nome = input("Que nome quer dar à tabela de vendas? ")

            table_consulta_nome = input("Que nome quer dar à tabela de consultas? ")

            table_filtro_nome = input("Que nome quer dar à tabela de filtros? ")

            table_resultado_nome = input("Que nome quer dar à tabela de resultados? ")

            table_dataset_nome = input("Que nome quer dar à tabela de dataset? ")

            # Queries para criar tabelas no banco se não existirem
            create_query1 = f"CREATE TABLE IF NOT EXISTS {table_medicamento_nome}(IDMEDICAMENTO INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,PRINCIPIOATIVO VARCHAR(380) NOT NULL,CLASSETERAPEUTICA VARCHAR(101) NOT NULL,NOMEPRODUTO VARCHAR(50) NOT NULL);"
            create_query2 = f"CREATE TABLE IF NOT EXISTS {table_filtro_nome}(IDFILTRO INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,TIPO ENUM('CLASSETERAPEUTICA', 'PRINCIPIOATIVO', 'NOMEPRODUTO') NOT NULL,CONTEUDO VARCHAR(380));"
            create_query3 = f"CREATE TABLE IF NOT EXISTS {table_dataset_nome}(IDDATASET INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,NOMEDATASET VARCHAR(7) UNIQUE NOT NULL,ANODATASET CHAR(4) NOT NULL,MESDATASET CHAR(2) NOT NULL);"
            create_query4 = f"CREATE TABLE IF NOT EXISTS {table_venda_nome}(IDVENDA INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,QTDVENDIDA INT NOT NULL,ANOVENDA CHAR(4) NOT NULL,MESVENDA CHAR(2) NOT NULL, PRINCIPIOATIVO VARCHAR(380) NOT NULL, ID_DATASET INT UNSIGNED NOT NULL,ID_MEDICAMENTO INT UNSIGNED UNIQUE);"
            create_query5 = f"CREATE TABLE IF NOT EXISTS {table_resultado_nome}(IDRESULTADO INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,SOMA INT UNSIGNED NOT NULL,ID_FILTRO INT UNSIGNED NOT NULL,ID_VENDA INT UNSIGNED NOT NULL);"
            create_query6 = f"CREATE TABLE IF NOT EXISTS {table_consulta_nome}(IDCONSULTA INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,ID_DATASET INT UNSIGNED NOT NULL,ID_RESULTADO INT UNSIGNED NOT NULL);"
            create_query7 = "CREATE TABLE IF NOT EXISTS ACESSA(ID_VENDA INT UNSIGNED NOT NULL,ID_RESULTADO INT UNSIGNED NOT NULL,PRIMARY KEY(ID_VENDA, ID_RESULTADO));"

            cursor.execute(create_query1)
            cursor.execute(create_query2)
            cursor.execute(create_query3)
            cursor.execute(create_query4)
            cursor.execute(create_query5)
            cursor.execute(create_query6)
            cursor.execute(create_query7)
        else:
            table_medicamento_nome = input("Qual o nome da tabela de medicamentos? ")

            table_venda_nome = input("Qual o nome da tabela de filtros? ")

            table_consulta_nome = input("Qual o nome da tabela de datasets? ")

            table_filtro_nome = input("Qual o nome da tabela de vendas? ")

            table_resultado_nome = input("Qual o nome da tabela de resultados? ")

            table_resultado_nome = input("Qual o nome da tabela de consultas? ")

            try:
                # Criando o dataframe de medicamentos e mandando para o banco
                df_medicamento = pd.read_csv(med_file, usecols = ['NOME_PRODUTO', 'CLASSE_TERAPEUTICA', 'PRINCIPIO_ATIVO'], delimiter = ";", quotechar = '"', quoting = 3, encoding = 'utf-8', encoding_errors = 'ignore')

                engine = create_engine(f"mysql+pymysql://{user}:{pw}@localhost/{db}")

                df_medicamento.to_sql(table_medicamento_nome, con = engine, if_exists = 'replace')
            except pd.error() as e:
                print(f'Erro: {e}')
                sys.exit(1)

            # Instanciando as tabelas criadas em variáveis - COLOCAR NOMES DAS COLUNAS
            table_consulta_query = f"SELECT * FROM {table_consulta_nome};"
            table_medicamento_query = f"SELECT IDMEDICAMENTO, NOME_PRODUTO, CLASSE_TERAPEUTICA, PRINCIPIO_ATIVO FROM {table_medicamento_nome};"
            table_filtro_query = f"SELECT IDFILTRO, TIPOFILTRO, CONTEUDOFILTRO FROM {table_filtro_nome};"
            table_resultado_query = f"SELECT MES, ANO, QTD FROM {table_resultado_nome};"

            cursor.execute(table_medicamento_query)
            cursor.execute(table_consulta_query)
            cursor.execute(table_filtro_query)
            cursor.execute(table_resultado_query)

            table_medicamento = []
            table_consulta = []
            table_filtro = []
            table_resultado = []

            for (IDMEDICAMENTO, NOME_PRODUTO, CLASSE_TERAPEUTICA, PRINCIPIO_ATIVO) in cursor:
                table_medicamento.append(IDMEDICAMENTO, NOME_PRODUTO, CLASSE_TERAPEUTICA, PRINCIPIO_ATIVO)
            for (IDFILTRO, TIPOFILTRO, CONTEUDOFILTRO) in cursor:
                table_filtro.append(IDFILTRO, TIPOFILTRO, CONTEUDOFILTRO)
            for (MES, ANO, QTD) in cursor:
                table_resultado.append(MES, ANO, QTD)

        # Seleção dos datasets de venda para mesclar no file_venda
        periodos = (int(input("Quantos períodos gostaria de incluir na consulta? ")))
        datasets = []
        for i in range(1, periodos+1):
            mes = input(f"Qual o mês do período {i}? ")
            ano = input(f"Qual o ano do período {i}? ")
            periodo = f"{mes}-{ano}.csv"
            pasta = "src/entities/datasets"
            for diretorio, subpastas, arquivos in os.walk(pasta):
                for arquivo in arquivos:
                    if arquivo.endswith(periodo):
                        datasets.append(arquivo)
        for periodo in datasets:
            periodo = 'src/entities/dataset/' + periodo

        try:
            df_dataset = pd.concat(map(pd.read_csv, datasets), ignore_index=True, axis= {'NOME_PRODUTO', 'CLASSE_TERAPEUTICA', 'PRINCIPIO_ATIVO'})

            # Criando um dataframe baseado no dataset de vendas
            df_venda = pd.read_csv(df_dataset, usecols = ['NOME_PRODUTO', 'CLASSE_TERAPEUTICA', 'PRINCIPIO_ATIVO'], delimiter = ";", quotechar = "'", quoting = (3), doublequote = False, encoding = 'utf-8', encoding_errors = 'ignore')

            # Conectando com o banco de dados para enviar dataframe
            engine = create_engine(f"mysql+pymysql://{user}:{pw}@localhost/{db}")

            # Enviando o dataframe de vendas para a tabela pertinente
            df_venda.to_sql(table_venda_nome, con = engine, if_exists = 'replace')

        except pd.error() as e:
            print(f'Erro: {e}')
            sys.exit(1)

        consulta_quest = input("Gostaria de iniciar uma nova consulta?(s/n)")

        if consulta_quest == "s":
            view_filtro_nome = input("Como gostaria de nomear a view de medicamentos? ")

            # Selecionando as colunas a serem visualizadas na tabela filtrada de medicamentos
            colunas_escolhidas_visual = list(input("Quais colunas gostaria de visualizar?(CLASSE_TERAPEUTICA, PRINCIPIO_ATIVO, NOME_PRODUTO) "))
            colunas = ["CLASSE_TERAPEUTICA", "PRINCIPIO_ATIVO", "NOME_PRODUTO"]
            if colunas_escolhidas_visual != "CLASSE_TERAPEUTICA" or colunas_escolhidas_visual != "PRINCIPIO_ATIVO" or colunas_escolhidas_visual != "NOME_PRODUTO":
                print("Coluna não existe.")
            colunas_filtradas = len(colunas_escolhidas_visual) * []

            restart = True
            while restart:
                restart = False
                columns_error = 0
                for i in enumerate(colunas_filtradas):
                    if columns_error >= len(colunas_filtradas):
                        restart = True
                        break
                    for coluna in colunas:
                        for coluna_escolhida in colunas_escolhidas_visual:
                            if coluna_escolhida == coluna:
                                colunas_filtradas = colunas_filtradas.append(coluna_escolhida)
                            else:
                                print(f"Coluna {coluna_escolhida} não existe.")
                                columns_error += 1

                colunas_filtradas = ", ".join(colunas_filtradas)    # Mostra as colunas que serão vistas na tabela filtrada

            # Selecionando os filtros
            qtd_filtros = (int(input("Quantos filtros quer? ")))

            counter = 1
            filtros_extra = ""
            view_filtro_list = []

            while counter <= qtd_filtros:

                tipo_filtro = input(f"Qual o tipo do filtro {counter}? ")

                conteudo_filtro = input(f"Qual o conteúdo do filtro {counter}? ")

                filtro = f"{tipo_filtro},{conteudo_filtro};"

                filtro_str = f"M.{tipo_filtro} LIKE '%{conteudo_filtro}%',"

                filtros_extra += filtro_str

                view_filtro_list = view_filtro_list.append(filtro)

                counter += 1
            filtros_extra = filtros_extra.split(",")
            filtros_extra = " OR ".join(filtros_extra)   # Mostra o texto dos filtros selecionados

            tipos_filtro = []
            conteudos_filtro = []
            for filtro in view_filtro_list:
                tipo_filtro, conteudo_filtro = filtro.split(",")
                tipos_filtro.append(tipo_filtro) # Mostra a lista de tipos de filtros preenchida com a lista criada no loop de filtro
                conteudos_filtro.append(conteudo_filtro) # Mostra a lista de conteudos de filtros preenchida com a lista criada no loop de filtro

            df_filtro = pd.Dataframe((zip(tipos_filtro, conteudos_filtro)), columns=['TIPO', 'CONTEUDO'])
            # Enviando o dataframe de filtros para a tabela pertinente
            df_filtro.to_sql(table_filtro_nome, con = engine, if_exists = 'append')

            # Escolhendo a coluna do order by
            order = input("Qual coluna gostaria de usar para ordenar a tabela?(NOME_PRODUTO, CLASSE_TERAPEUTICA, PRINCIPIO_ATIVO) ")
            order_by = ""
            for tipo_filtro in tipos_filtro:
                if order == tipo_filtro:
                    order_by = order    # Mostra a coluna usada para o order by

            # Query da view filtrada
            view_filtro_query = f"CREATE VIEW IF NOT EXISTS {view_filtro_nome} AS SELECT {colunas_filtradas} FROM {table_medicamento_nome} WHERE {filtros_extra} ORDER BY {order_by};"

            view_resultado_nome = input("Como gostaria de nomear a visualização do resultado? ")

            resultado_query = f"SELECT MES_VENDA AS MES, ANO_VENDA AS ANO, SUM(QTD_VENDIDA) AS QTD FROM {table_venda_nome} INNER JOIN {view_filtro_nome} ON {view_filtro_nome}.PRINCIPIO_ATIVO={table_venda_nome}.PRINCIPIO_ATIVO WHERE {table_medicamento_nome}.{tipo_filtro} LIKE '{conteudo_filtro}%' {filtros_extra} AND {table_venda_nome}.PRINCIPIO_ATIVO IS NOT NULL AND {table_medicamento_nome}.PRINCIPIO_ATIVO IS NOT NULL;"

            cursor.execute(resultado_query)

            # Usar resultado do execute para criar um dataframe
            resultado = []
            for (MES, ANO, QTD) in cursor:
                resultado.append((ANO, MES, QTD))

            df_resultado = pd.Dataframe(resultado, columns=['QTD', 'ANO', 'MES'])

            df_resultado.to_sql(table_consulta_nome, con = engine, if_exists = 'append')

            print(df_resultado)

        else:
            conn = connection()
            cursor = conn.cursor()

            table_consulta_nome = input("Qual o nome da tabela de consulta? ")

            table_consulta_query = f"SELECT C.IDCONSULTA, R.MES, R.ANO, R.SOMA, F.TIPOFILTRO, F.CONTEUDOFILTRO, D.NOMEDATASET FROM {table_consulta_nome} C INNER JOIN ({table_resultado_nome} R, {table_dataset_nome} D, {table_venda_nome} V, {table_filtro_nome} F) ON (C.IDCONSULTA = R.ID_CONSULTA AND V.ID_DATASET = D.IDDATASET AND C.IDCONSULTA = F.ID_CONSULTA) ORDER BY {order_by};"
            cursor.execute(table_consulta_query)

            table_consulta = []

            for (IDCONSULTA, MES, ANO, QTD) in cursor:
                table_consulta.append((IDCONSULTA, MES, ANO, QTD))

            old_consulta = input("Gostaria de acessar uma consulta anterior?(s/n) ")

            if old_consulta.lower() == "s":
                id_consulta = input("Qual o id da consulta que você gostaria de acessar? ")

                for consulta in table_consulta:
                    if table_consulta[0] == id_consulta:
                        print(consulta)

        quest = input("Gostaria de tentar novamente?(s/n)")

        if quest.lower() == 's':
            continue
        else:
            sys.exit()
