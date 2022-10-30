import os
from numpy import int64
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

    venda_file2 = 'src\\entities\\datasets\\venda_teste.csv'
    bebida_file1 = 'src\\entities\\datasets\\bebidas.csv'

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

    try:
        df_venda.to_sql('venda', con=engine, index=True, index_label='IDVENDA', if_exists='fail', method='multi')

        print(f"Dataframe de venda enviado para o banco de dados '{db}'")

    except ValueError:
        print("Tabela 'venda' já existe.")

    usuario = [user]

    # Fazer csv dos dados para criar dataframes

    df_filtro = pd.DataFrame(columns=['TIPO', 'CONTEUDO', 'ID_CONSULTA'])
    df_periodo = pd.DataFrame(columns=['ANOINICIAL', 'ANOFINAL', 'ID_CONSULTA'])
    df_consulta = pd.DataFrame(columns=['ID_USUARIO', 'SOMA'])
    df_usuario = pd.Dataframe(user, columns=['LOGIN'])

    df_join = df_venda.join(df_bebida, on='ID_BEBIDA', how='inner', lsuffix='_VENDA', rsuffix='_BEBIDA')

    # Seleção dos periodos de venda para filtrar
    anoinicial = int(input("Qual o número do ano inicial? "))
    anofinal = int(input("Qual o número do ano final?"))

    periodo = [anoinicial, anofinal, 0]

    df_periodo.append(periodo)

    df_join = df_join[(df_join['DATAVENDA'].dt.year >= anoinicial) & (df_join['DATAVENDA'].dt.year <= anofinal)]
    print(df_join)

    tipo = input("Qual o tipo do filtro?(CATEGORIA/PRODUTO) ")
    conteudo = input("Qual o conteúdo do filtro? ")
    filtro = [tipo, conteudo, 0]

    df_filtro.append(filtro)

    df = df_join[(df_join[f'{tipo}'] == conteudo)]

    soma = df.sum(axis='VOLUMEVENDIDO')
    df_consulta.append(0, soma)

    print(df_usuario)

    pd.set_option('display.max_rows', 20)
    print(df_filtro)
