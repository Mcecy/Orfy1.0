import os
import sys
import pwinput as pw
from sqlalchemy import create_engine
import pandas as pd
import mariadb
import filtros


class App:

    print("Boas-vindas a OrFy!")

    # Cria um diretório na pasta do sistema Program Files
    folder = "C:\\Program Files\\OrFy"

    exists = os.path.exists(folder)

    if not exists:
        os.makedirs(folder)

    med_file = 'src\\entities\\datasets\\DADOS_ABERTOS_MEDICAMENTOS.csv'

    while True:
        restart = True
        while restart:
            restart = False

            # Criando as tabelas a serem utilizadas
            table_quest = input("Gostaria de criar novas tabelas?(s/n)")

            if table_quest.lower() == 's':
                # Conexão com o banco de dados no mariadb
                user = input("Digite seu usuário: ")
                pw = pw.pwinput(prompt='Digite sua senha: ')

                port_quest = input("Gostaria de usar port padrão?(s/n) ")

                port = 3306

                if port_quest.lower() == "n":
                    port = (int(input("Digite o port de sua escolha: ")))

                db = input("Digite o nome da sua database: ")

                conn = mariadb.connect(user=user, password=pw, port=port, database=db)

                try:
                    # Iniciando cursor para manipulação do banco
                    cursor = conn.cursor()
                except mariadb.Error as e:
                    print(f"Erro ao conectar com o MariaDB: {e}")
                    sys.exit(1)

                # Queries para criar tabelas no banco se não existirem
                create_query1 = "CREATE TABLE IF NOT EXISTS medicamento(IDMEDICAMENTO INT UNSIGNED PRIMARY KEY AUTO_INCREMENT, NUMEROREGISTRO VARCHAR(8) NOT NULL, PRINCIPIOATIVO VARCHAR(380) NOT NULL,CLASSETERAPEUTICA VARCHAR(101) NOT NULL,NOMEPRODUTO VARCHAR(50) NOT NULL);"
                create_query2 = "CREATE TABLE IF NOT EXISTS filtro(IDFILTRO INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,TIPOFILTRO ENUM('CLASSETERAPEUTICA', 'PRINCIPIOATIVO', 'NOMEPRODUTO') NOT NULL,CONTEUDOFILTRO VARCHAR(380));"
                create_query3 = "CREATE TABLE IF NOT EXISTS dataset(IDDATASET INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,NOMEDATASET VARCHAR(7) UNIQUE NOT NULL,ANODATASET CHAR(4) NOT NULL,MESDATASET CHAR(2) NOT NULL);"
                create_query4 = "CREATE TABLE IF NOT EXISTS venda(IDVENDA INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,QTDVENDIDA INT NOT NULL,ANOVENDA CHAR(4) NOT NULL,MESVENDA CHAR(2) NOT NULL, NUMEROREGISTRO VARCHAR(8) NOT NULL, ID_DATASET INT UNSIGNED NOT NULL,ID_MEDICAMENTO INT UNSIGNED NOT NULL);"
                create_query5 = "CREATE TABLE IF NOT EXISTS consulta(IDCONSULTA INT UNSIGNED PRIMARY KEY AUTO_INCREMENT, SOMA BIGINT(20) NOT NULL, ID_FILTRO INT UNSIGNED NOT NULL, ID_VENDA INT UNSIGNED NOT NULL);"

                cursor.execute(create_query1)
                cursor.execute(create_query2)
                cursor.execute(create_query3)
                cursor.execute(create_query4)
                cursor.execute(create_query5)

                # Criando o dataframe de medicamentos e mandando para o banco
                df_medicamento = pd.read_csv(med_file, delimiter=";", header=0, names=['1', 'NOMEPRODUTO', '2', '3', "NUMEROREGISTRO", "5", "6", "CLASSETERAPEUTICA", "7", '8', 'PRINCIPIOATIVO'], usecols=['NUMEROREGISTRO', 'NOMEPRODUTO', 'CLASSETERAPEUTICA', 'PRINCIPIOATIVO'], quotechar="'\"", quoting=3, encoding='utf-8', encoding_errors='ignore')

                engine = create_engine(f"mysql+pymysql://{user}:{pw}@localhost/{db}")

                df_medicamento.to_sql('medicamento', con=engine, if_exists='replace')

            elif table_quest.lower() == 'n':
                restart = True
                while restart:
                    restart = False

                    consulta_quest = input("Gostaria de iniciar uma nova consulta?(s/n)")

                    if consulta_quest == "s":
                        # Conexão com o banco de dados no mariadb
                        user = input("Digite seu usuário: ")
                        pw = pw.pwinput(prompt='Digite sua senha: ')

                        port_quest = input("Gostaria de usar port padrão?(s/n) ")

                        port = 3306

                        if port_quest.lower() == "n":
                            port = (int(input("Digite o port de sua escolha: ")))

                        db = input("Digite o nome da sua database: ")

                        conn = mariadb.connect(user=user, password=pw, port=port, database=db)

                        try:
                            # Iniciando cursor para manipulação do banco
                            cursor = conn.cursor()
                        except mariadb.Error as e:
                            print(f"Erro ao conectar com o MariaDB: {e}")
                            sys.exit(1)

                        try:
                            # Criando o dataframe de medicamentos e mandando para o banco
                            df_medicamento = pd.read_csv(med_file, delimiter=";", header=0, names=['1', 'NOMEPRODUTO', '2', '3', "NUMEROREGISTRO", "5", "6", "CLASSETERAPEUTICA", "7", '8', 'PRINCIPIOATIVO'], usecols=['NUMEROREGISTRO', 'NOMEPRODUTO', 'CLASSETERAPEUTICA', 'PRINCIPIOATIVO'], quotechar="'\"", quoting=3, encoding='utf-8', encoding_errors='ignore')

                            engine = create_engine(f"mysql+pymysql://{user}:{pw}@localhost/{db}")

                            df_medicamento.to_sql('medicamento', con=engine, if_exists='replace')
                        except pd.error() as e:
                            print(f'Erro: {e}')
                            sys.exit(1)

                        # Instanciando as tabelas criadas em variáveis
                        dataset_query = "SELECT IDDATASET, NOMEDATASET, MESDATASET, ANODATASET FROM dataset;"
                        medicamento_query = "SELECT IDMEDICAMENTO, NOMEPRODUTO, CLASSETERAPEUTICA, PRINCIPIOATIVO FROM medicamento;"
                        filtro_query = "SELECT IDFILTRO, TIPOFILTRO, CONTEUDOFILTRO FROM filtro;"
                        consulta_query = "SELECT IDCONSULTA, SOMA, ID_FILTRO, ID_VENDA FROM consulta"
                        venda_query = "SELECT IDVENDA, NUMEROREGISTRO, MESVENDA, ANOVENDA, QTDVENDIDA, ID_DATASET, ID_MEDICAMENTO FROM venda;"

                        cursor.execute(medicamento_query)
                        cursor.execute(consulta_query)
                        cursor.execute(filtro_query)
                        cursor.execute(venda_query)
                        cursor.execute(dataset_query)

                        table_medicamento = []
                        table_consulta = []
                        table_filtro = []
                        table_venda = []
                        table_dataset = []

                        for (IDMEDICAMENTO, NOMEPRODUTO, CLASSETERAPEUTICA, PRINCIPIOATIVO) in cursor:
                            table_medicamento.append((IDMEDICAMENTO, NOMEPRODUTO, CLASSETERAPEUTICA, PRINCIPIOATIVO))
                        for (IDFILTRO, TIPOFILTRO, CONTEUDOFILTRO) in cursor:
                            table_filtro.append((IDFILTRO, TIPOFILTRO, CONTEUDOFILTRO))
                        for (IDCONSULTA, SOMA, ID_FILTRO, ID_VENDA) in cursor:
                            table_consulta.append((IDCONSULTA, SOMA, ID_FILTRO, ID_VENDA))
                        for (IDVENDA, NUMEROREGISTRO, MESVENDA, ANOVENDA, QTDVENDIDA, ID_DATASET, ID_MEDICAMENTO) in cursor:
                            table_venda.append((IDVENDA, NUMEROREGISTRO, MESVENDA, ANOVENDA, QTDVENDIDA, ID_DATASET, ID_MEDICAMENTO))

                        # Seleção dos datasets de venda para mesclar e gerar dataset base
                        periodos = (int(input("Quantos períodos gostaria de incluir na consulta? ")))
                        datasets = []
                        for i in range(1, periodos + 1):
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
                            cursor.execute('TRUNCATE TABLE venda;')

                            for dataset in datasets:
                                df_venda = pd.read_csv(dataset, delimiter=";", header=0, names=["ANOVENDA", "MESVENDA", "1", "2", "PRINCIPIOATIVO", "3", "QTDVENDIDA", "4", "5", "6", "7", "8", "9", "10", "11"], usecols=['QTDVENDIDA', 'ANOVENDA', 'MESVENDA', 'PRINCIPIOATIVO'], quotechar="'\"", quoting=3, doublequote=False, encoding='utf-8', encoding_errors='ignore')
                                df_venda.to_sql('venda', con=engine, if_exists='append')
                        except pd.error() as e:
                            print(f'Erro: {e}')
                            sys.exit(1)

                        update1 = "UPDATE venda V INNER JOIN medicamento M ON V.NUMERO_REGISTRO = M.NUMERO_REGISTRO SET V.ID_MEDICAMENTO = M.IDMEDICAMENTO WHERE V.NUMERO_REGISTRO = M.NUMERO_REGISTRO;"
                        update2 = "UPDATE venda V INNER JOIN dataset D ON (V.MESVENDA = D.MESDATASET AND V.ANOVENDA = D.ANODATASET) SET V.ID_DATASET = D.IDDATASET WHERE V.MESVENDA = D.MESDATASET AND V.ANOVENDA = D.ANODATASET;"
                        cursor.execute(update1)
                        cursor.execute(update2)

                        alter_query1 = "ALTER TABLE vendaADD CONSTRAINT FK_VENDA_MEDICAMENTO FOREIGN KEY(ID_MEDICAMENTO) REFERENCES MEDICAMENTO(IDMEDICAMENTO);"
                        alter_query2 = "ALTER TABLE venda ADD CONSTRAINT FK_VENDA_DATASET FOREIGN KEY(ID_DATASET) REFERENCES DATASET_VENDAS(IDDATASET);"
                        alter_query3 = "ALTER TABLE consulta ADD CONSTRAINT FK_CONSULTA_VENDA FOREIGN KEY(ID_VENDA) REFERENCES VENDA(IDVENDA);"
                        cursor.execute(alter_query1)
                        cursor.execute(alter_query2)
                        cursor.execute(alter_query3)

                        view_nome = input("Como gostaria de nomear a view de medicamentos? ")

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
                                            colunas_filtradas.append(coluna_escolhida)
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

                            filtros.filtros(filtros_extra, tipo_filtro, conteudo_filtro)

                            filtros.filtrolist(view_filtro_list, filtro, tipo_filtro, conteudo_filtro)

                            counter += 1
                        filtros_extra = filtros_extra.split(",")
                        filtros_extra = " OR ".join(filtros_extra)   # Mostra o texto dos filtros selecionados

                        tipos_filtro = []
                        conteudos_filtro = []
                        for filtro in view_filtro_list:
                            tipo_filtro, conteudo_filtro = filtro.split(",")
                            tipos_filtro.append(tipo_filtro)  # Mostra a lista de tipos de filtros preenchida com a lista criada no loop de filtro
                            conteudos_filtro.append(conteudo_filtro)  # Mostra a lista de conteudos de filtros preenchida com a lista criada no loop de filtro

                        df_filtro = pd.Dataframe((zip(tipos_filtro, conteudos_filtro)), columns=['TIPO', 'CONTEUDO'])
                        # Enviando o dataframe de filtros para a tabela pertinente
                        df_filtro.to_sql('filtro', con=engine, if_exists='append')

                        order = input("Qual coluna gostaria de usar para ordenar a tabela?(NOME_PRODUTO, CLASSE_TERAPEUTICA, PRINCIPIO_ATIVO) ")

                        order_by = filtros.orderby(order, tipos_filtro)

                        # Query da view filtrada
                        view_filtro_query = f"CREATE VIEW IF NOT EXISTS {view_nome} AS SELECT {colunas_filtradas} FROM medicamento M WHERE {filtros_extra} ORDER BY {order_by};"

                        view_consulta_nome = input("Como gostaria de nomear a visualização do resultado? ")

                        consulta_query = f'SELECT C.IDCONSULTA, V.MESVENDA AS MES, V.ANOVENDA AS ANO, C.SOMA, F.TIPOFILTRO, F.CONTEUDOFILTRO, D.NOMEDATASET FROM consulta C INNER JOIN (dataset D,venda V,filtro F) ON (C.ID_VENDA = V.IDVENDAAND V.ID_DATASET = D.IDDATASETAND C.IDCONSULTA = F.ID_CONSULTA) ORDER BY {order_by};'

                        cursor.execute(consulta_query)

                        # Usar resultado do execute para criar um dataframe
                        consulta = []
                        for (IDCONSULTA, MESVENDA, ANOVENDA, SOMA, TIPOFILTRO, CONTEUDOFILTRO, NOMEDATASET) in cursor:
                            consulta.append((IDCONSULTA, MESVENDA, ANOVENDA, SOMA, TIPOFILTRO, CONTEUDOFILTRO, NOMEDATASET))

                        df_consulta = pd.Dataframe(consulta, header=0, columns=['IDCONSULTA', 'MESVENDA', 'ANOVENDA', 'SOMA', 'TIPOFILTRO', 'CONTEUDOFILTRO', 'NOMEDATASET'])

                        df_consulta.to_sql('consulta', con=engine, if_exists='append')

                        cursor.execute("SELECT MES, ANO, SOMA, TIPOFILTRO, CONTEUDOFILTRO, DATASET FROM consulta WHERE IDCONSULTA = LAST_INSERT_ID();")

                        for (IDCONSULTA, MESVENDA, ANOVENDA, SOMA, TIPOFILTRO, CONTEUDOFILTRO, NOMEDATASET) in cursor:
                            print((IDCONSULTA, MESVENDA, ANOVENDA, SOMA, TIPOFILTRO, CONTEUDOFILTRO, NOMEDATASET))

                    elif consulta_quest.lower() == 'n':
                        restart = True
                        while restart:
                            restart = False
                            old_consulta = input("Gostaria de acessar uma consulta anterior?(s/n) ")

                            if old_consulta.lower() == "s":
                                # Conexão com o banco de dados no mariadb
                                user = input("Digite seu usuário: ")
                                pw = pw.pwinput(prompt='Digite sua senha: ')

                                port_quest = input("Gostaria de usar port padrão?(s/n) ")

                                port = 3306

                                if port_quest.lower() == "n":
                                    port = (int(input("Digite o port de sua escolha: ")))

                                db = input("Digite o nome da sua database: ")

                                conn = mariadb.connect(user=user, password=pw, port=port, database=db)

                                try:
                                    # Iniciando cursor para manipulação do banco
                                    cursor = conn.cursor()
                                except mariadb.Error as e:
                                    print(f"Erro ao conectar com o MariaDB: {e}")
                                    sys.exit(1)

                                id_consulta = input("Qual o id da consulta que você gostaria de acessar? ")

                                cursor = conn.cursor()

                                restart = True
                                consulta_query = ""
                                while restart:
                                    restart = False

                                    order_by = input("Gostaria de usar a ordenação padrão(ANO)?(s/n)")

                                    if order_by.lower() == 's':
                                        consulta_query = f"SELECT C.IDCONSULTA, V.MESVENDA AS MES, V.ANOVENDA AS ANO, R.SOMA, F.TIPOFILTRO, F.CONTEUDOFILTRO, D.NOMEDATASET FROM consulta C INNER JOIN (resultado R, dataset D, venda V, filtro F) ON (C.IDCONSULTA = R.ID_CONSULTA AND V.ID_DATASET = D.IDDATASET AND C.IDCONSULTA = F.ID_CONSULTA) WHERE {filtros_extra} AND (V.PRINCIPIO_ATIVO IS NOT NULL AND M.PRINCIPIO_ATIVO IS NOT NULL) ORDER BY V.ANOVENDA;"
                                    elif order_by.lower() == 'n':
                                        order_by = input('Quais colunas gostaria de usar para ordernar a visualização?(Separe por vírgula) ')
                                        consulta_query = f"SELECT C.IDCONSULTA, V.MESVENDA AS MES, V.ANOVENDA AS ANO, R.SOMA, F.TIPOFILTRO, F.CONTEUDOFILTRO, D.NOMEDATASET FROM consulta C INNER JOIN (resultado R, dataset D, venda V, filtro F) ON (C.IDCONSULTA = R.ID_CONSULTA AND V.ID_DATASET = D.IDDATASET AND C.IDCONSULTA = F.ID_CONSULTA) WHERE {filtros_extra} AND (V.PRINCIPIO_ATIVO IS NOT NULL AND M.PRINCIPIO_ATIVO IS NOT NULL) ORDER BY {order_by};"
                                    else:
                                        print('Resposta inválida.')
                                        restart = True

                                cursor.execute(consulta_query)

                                table_consulta = []

                                for (IDCONSULTA, MESVENDA, ANOVENDA, SOMA, TIPOFILTRO, CONTEUDOFILTRO, NOMEDATASET) in cursor:
                                    table_consulta.append((IDCONSULTA, MESVENDA, ANOVENDA, SOMA, TIPOFILTRO, CONTEUDOFILTRO, NOMEDATASET))

                                for linha in table_consulta:
                                    for campo in coluna:
                                        if table_consulta[linha][campo] == id_consulta:
                                            print(consulta)
                            elif old_consulta.lower() == 'n':
                                continue
                            else:
                                print("Essa resposta não é válida.")
                                restart = True
                    else:
                        print('Essa resposta não é válida.')
                        restart = True
            else:
                print("Essa resposta não é válida.")
                restart = True

        quest = input("Gostaria de tentar novamente?(s/n)")

        restart = True
        while restart:
            restart = False
            if quest.lower() == 's':
                continue
            elif quest.lower() == 'n':
                sys.exit()
            else:
                print('Essa resposta não é válida.')
                restart = True
