import os
from numpy import int64
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

    # Conexão com o banco de dados no mariadb
    user = input("Digite seu usuário: ")
    pw = pw.pwinput(prompt='Digite sua senha: ')

    port_quest = input("Gostaria de usar port padrão?(s/n) ")

    db = input("Digite o nome da sua database: ")

    conn = mariadb.connect(user=user, password=pw, port=3306, database=db)
    cursor = conn.cursor()
    cursor.fast_executemany = True

    # Engine usada ao passar os dataframes para o banco de dados
    engine = create_engine(f"mysql+pymysql://{user}:{pw}@localhost/{db}")

    con = engine.connect()

    venda_file1 = 'src\\entities\\datasets\\vendas_bebida.csv'
    venda_file2 = 'src\\entities\\datasets\\venda_teste.csv'
    bebida_file1 = 'src\\entities\\datasets\\bebidas.csv'
    bebida_file2 = 'src\\entities\\datasets\\beverages.csv'

    # Criando o dataframe de bebidas e mandando para o banco
    df_bebida = pd.read_csv(bebida_file1, delimiter=',', header=0, names=['CODIGO', 'CATEGORIA', 'NOME', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17'], usecols=['NOME', 'CATEGORIA', 'CODIGO'], quoting=3, encoding='utf-8', encoding_errors='ignore')

    print("Dataframe 'bebida' criado.")

    try:
        df_bebida.to_sql('bebida', con=engine, index=True, index_label='IDBEBIDA', if_exists='fail')

        print(f"'bebida' foi enviado para o banco de dados '{db}'")
    except ValueError:
        print("Tabela 'bebida' já existe.")

    # Criando o dataframe de vendas e mandando para o banco
    dict_venda = {'DATAVENDA': str, 'NOMEPRODUTO': str, 'CODIGO': int, 'VOLUMEVENDIDO': float}

    df_venda = pd.read_csv(venda_file2, delimiter=',', header=0, names=["1", "DATAVENDA", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "CODIGO", "PRODUTO", "17", "18", "19", "20", "21", "22", "VOLUMEVENDIDO", "24"], usecols=['DATAVENDA', 'CODIGO', 'PRODUTO', 'VOLUMEVENDIDO'], quoting=3, encoding='utf-8', encoding_errors='ignore')

    print("Dataframe de venda criado.")

    # df_venda['DATAVENDA'] = lookup(df_venda['DATAVENDA'], format='%m/%d/%Y')

    # print("Datas transformadas.")

    # pd.to_numeric(df_venda['CODIGO'], errors='coerce').dropna(inplace=True)

    for i, col in enumerate(df_venda.columns):
        if col == 'CODIGO':
            df_venda[f'{col}'] = df_venda[f'{col}'].replace(r'[^\d]', '', regex=True)
        if col == 'VOLUMEVENDIDO':
            df_venda[f'{col}'] = df_venda[f'{col}'].replace(r'[^\d.]', '', regex=True)
        if col == 'DATAVENDA' or col == 'PRODUTO':
            df_venda[f'{col}'] = df_venda[f'{col}'].replace('"', '', regex=True)

    df_venda['CODIGO'] = pd.to_numeric(df_venda['CODIGO'])

    df_venda['CODIGO'] = df_venda['CODIGO'].values.astype(int64)

    df_venda['DATAVENDA'] = pd.to_datetime(df_venda['DATAVENDA'])
    df_venda['VOLUMEVENDIDO'] = pd.to_numeric(df_venda['VOLUMEVENDIDO'])

    df_venda['ID_BEBIDA'] = pd.NA

    print("Códigos, datas e volumes transformados.")

    for i in df_venda.index:
        for ind in df_bebida.index:
            if df_venda.at[i, 'CODIGO'] == df_bebida.at[ind, 'CODIGO']:
                df_venda.at[i, 'ID_BEBIDA'] = ind

    df_venda.dropna(inplace=True)

    df_venda['ID_BEBIDA'] = df_venda['ID_BEBIDA'].astype(int64)

    print("Aguarde alguns instantes.")
    '''
    # Seleção dos periodos de venda para filtrar
    mesinicial = 0
    anoinicial = 0
    mesfinal = 0
    anofinal = 0
    for i in range(0, 2):
        if i == 0:
            mesinicial = int(input("Qual o número do mês inicial? "))
            anoinicial = int(input("Qual o número do ano inicial? "))
        if i == 1:
            mesfinal = int(input("Qual o número do mês final?"))
            anofinal = int(input("Qual o número do ano final?"))

    df_periodo = df_venda[(df_venda['DATAVENDA'].dt.month >= mesinicial) & (df_venda['DATAVENDA'].dt.year >= anoinicial) & (df_venda['DATAVENDA'].dt.month <= mesfinal) & (df_venda['DATAVENDA'].dt.year <= anofinal)]

    try:


        # Criando um dataframe baseado no dataset de vendas
        df_venda = pd.read_csv(df_dataset, usecols = ['NOME_PRODUTO', 'CLASSE_TERAPEUTICA', 'PRINCIPIO_ATIVO'], delimiter = ";", quotechar = "'", quoting = (3), doublequote = False, encoding = 'utf-8', encoding_errors = 'ignore')
    '''
    try:
        df_venda.to_sql('venda', con=engine, index=True, index_label='IDVENDA', if_exists='fail', method='multi')

        print(f"Dataframe de venda enviado para o banco de dados '{db}'")

    except ValueError:
        print("Tabela 'venda' já existe.")

    # Seleção dos periodos de venda para filtrar
    mesinicial = int(input("Qual o número do mês inicial? "))
    anoinicial = int(input("Qual o número do ano inicial? "))
    mesfinal = int(input("Qual o número do mês final?"))
    anofinal = int(input("Qual o número do ano final?"))

    df_periodo = df_venda[(df_venda['DATAVENDA'].dt.month >= mesinicial) & (df_venda['DATAVENDA'].dt.year >= anoinicial) & (df_venda['DATAVENDA'].dt.month <= mesfinal) & (df_venda['DATAVENDA'].dt.year <= anofinal)]

    qtd_filtros = int(input("Qual a quantidade de filtros?"))
    filtros = filtros.filtros(qtd_filtros)
    tipos_filtro, conteudos_filtro = filtros.filtros_list(filtros)

    filters = []
    for tipo in tipos_filtro:
        for conteudo in conteudos_filtro:
            for value in df_periodo[f'{tipo}'].values:
                if tipo in df_periodo.columns and conteudo in value:
                    filter = (df_periodo[f'{tipo}'] == conteudo)
                    filters.append(filter)
    #df_periodo[(df_periodo[f'{tipo}'] == conteudo)]
    df_filtro = 
