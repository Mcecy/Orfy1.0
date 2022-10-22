import os
import pwinput as pw
from sqlalchemy import create_engine
import pandas as pd
import mariadb
from parsedates import lookup
from re import search


class App:

    print("Boas-vindas a OrFy!")

    # Cria um diretório na pasta do sistema Program Files
    folder = "C:\\Program Files\\OrFy"

    exists = os.path.exists(folder)

    if not exists:
        os.makedirs(folder)

    # Conexão com o banco de dados no mariadb
    user = input("Digite seu usuário: ")
    pw = pw.pwinput(prompt='Digite sua senha: ')

    port_quest = input("Gostaria de usar port padrão?(s/n) ")

    port = 3306

    if port_quest.lower() == "n":
        port = (int(input("Digite o port de sua escolha: ")))

    db = input("Digite o nome da sua database: ")

    conn = mariadb.connect(user=user, password=pw, port=port, database=db)

    cursor = conn.cursor()

    # Engine usada ao passar os dataframes para o banco de dados
    engine = create_engine(f"mysql+pymysql://{user}:{pw}@localhost/{db}")

    con = engine.connect()

    venda_file = 'src\\entities\\datasets\\venda_teste.csv'
    bebida_file = 'src\\entities\\datasets\\beverages.csv'

    # Criando o dataframe de bebidas e mandando para o banco
    df_bebida = pd.read_csv(bebida_file, delimiter=';', header=0, names=['NOMEPRODUTO', 'VOLUMEALCOOL'], usecols=['NOMEPRODUTO', 'VOLUMEALCOOL'], quoting=3, encoding='utf-8', encoding_errors='ignore')

    print("Dataframe 'bebida' criado.")

    try:
        df_bebida.to_sql('bebida', con=engine, index=True, index_label='IDBEBIDA', if_exists='fail')

        print(f"'bebida' foi enviado para o banco de dados '{db}'")
    except ValueError:
        print("Tabela 'bebida' já existe.")

    cursor.execute("UPDATE bebida SET NOMEPRODUTO = REPLACE(NOMEPRODUTO, ' , ', ' ');")

    # Criando o dataframe de vendas e mandando para o banco
    dict_venda = {'DATAVENDA': str, 'NOMEPRODUTO': str, 'VOLUMEVENDIDO': float}

    df_venda = pd.read_csv(venda_file, delimiter=',', header=0, names=['1', 'DATAVENDA', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', 'NOMEPRODUTO', '17', '18', '19', '20', '21', '22', 'VOLUMEVENDIDO', '24'], usecols=['DATAVENDA', 'NOMEPRODUTO', 'VOLUMEVENDIDO'], dtype=dict_venda, quoting=3, encoding='utf-8', encoding_errors='ignore')

    print("Dataframe de venda criado.")

    print("Aguarde alguns instantes. Se essa for a primeira vez rodando OrFy nesse DB, o sistema terminará o insert em aproximadamente 17min.")

    df_venda['DATAVENDA'] = lookup(df_venda['DATAVENDA'], format='%m/%d/%Y')

    print("Datas transformadas.")

    df_venda['ID_BEBIDA'] = 0

    # df_venda.loc[df_venda['NOMEPRODUTO'].eq(df_bebida, axis='NOMEPRODUTO'), 'ID_BEBIDA'] = df_bebida.loc[df_venda['NOMEPRODUTO'].eq(df_bebida['NOMEPRODUTO']), 'IDBEBIDA']

    '''
    for data, nome, volume, id_bebida in df_venda.values:
        for bebida, alcohol in df_bebida.values:
            id_bebida = df_bebida['IDBEBIDA'].values.where(df_venda['NOMEPRODUTO'].eq(df_bebida['NOMEPRODUTO']))

    for column in df_venda.columns:
        similar = False
        if column == 'NOMEPRODUTO':
            for value in df_venda.column.values:
                for bebida in df_bebida.columns:
                    if bebida == 'NOMEPRODUTO':
                        for nome in df_bebida.bebida.values:
                            if value == nome:
                                df_venda.id_bebida. = df_bebida

    for data, nome, volume, id_bebida in df_venda.values:
        for i, bebida, alcohol in bebidas:
            if nome == bebida:
                df_venda.loc[df_venda['NOMEPRODUTO'].values == df_bebida['NOMEPRODUTO'].values, 'ID_BEBIDA'] = i
    '''
    cursor.execute("SELECT IDBEBIDA, NOMEPRODUTO, VOLUMEALCOOL FROM bebida;")

    bebidas = []

    for (IDBEBIDA, NOMEPRODUTO, VOLUMEALCOOL) in cursor:
        bebidas.append(list((IDBEBIDA, NOMEPRODUTO, VOLUMEALCOOL)))

    df_venda = df_venda.values.tolist()

    for data, nome, volume, id_bebida in df_venda:
        for i, bebida, alcohol in bebidas:
            if search(nome, bebida):
                id_bebida = i

    df_venda = pd.DataFrame(df_venda, columns=['DATAVENDA', 'NOMEPRODUTO', 'VOLUMEVENDIDO', 'ID_BEBIDA'])

    try:
        df_venda.to_sql('venda', con=engine, index=True, index_label='IDVENDA', if_exists='fail')

        print(f"Dataframe de venda enviado para o banco de dados '{db}'")

    except ValueError:
        print("Tabela 'venda' já existe.")
