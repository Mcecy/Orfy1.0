import os
import pwinput as pw
from sqlalchemy import create_engine
import pandas as pd
import mariadb


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

    try:
        df_venda['CODIGO'] = pd.astype(pd.Int64Dtype)
    except:
        print("Impossível converter.")

    df_venda['DATAVENDA'] = pd.to_datetime(df_venda['DATAVENDA'])
    df_venda['VOLUMEVENDIDO'] = pd.to_numeric(df_venda['VOLUMEVENDIDO'])

    df_venda['ID_BEBIDA'] = pd.NA

    print("Códigos, datas e volumes transformados.")

    

    for i in df_venda.index:
        for ind in df_bebida.index:
            if df_venda.at[i, 'CODIGO'] == df_bebida.at[ind, 'CODIGO']:
                df_venda.at[i, 'ID_BEBIDA'] = df_bebida.at[ind, 'IDBEBIDA']

    print("Aguarde alguns instantes. Se essa for a primeira vez rodando OrFy nesse DB, o sistema terminará o insert em aproximadamente 17min.")
    '''
     # Seleção dos datasets de venda para mesclar no file_venda
    periodos = (int(input("Quantos períodos gostaria de incluir na consulta? ")))
    datasets = []
    for i in range(1, 3):
        mes = input(f"Qual o mês do período {i}? ")
        ano = input(f"Qual o ano do período {i}? ")
        periodo = f"{mes}/{ano}"
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

    cursor.execute("DELETE FROM venda WHERE CODIGO IS NULL OR CODIGO = '';")
    print("Valores nulos apagados.")

    cursor.execute("UPDATE venda V INNER JOIN bebida B ON V.CODIGO = B.CODIGO SET V.ID_BEBIDA = B.IDBEBIDA WHERE V.CODIGO = B.CODIGO AND V.CODIGO IS NOT NULL AND B.CODIGO IS NOT NULL;")
    print("Update executado.")
    '''
    try:
        df_venda.to_sql('venda', con=engine, index=True, index_label='IDVENDA', if_exists='fail', method='multi')

        print(f"Dataframe de venda enviado para o banco de dados '{db}'")

    except ValueError:
        print("Tabela 'venda' já existe.")
