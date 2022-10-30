import pandas as pd

venda_file = 'src\\entities\\datasets\\sale.csv'
bebida_file = 'src\\entities\\datasets\\liquor.csv'
usuario_file = 'src\\entities\\datasets\\usuario.csv'
consulta_file = 'src\\entities\\datasets\\consulta.csv'
filtro_file = 'src\\entities\\datasets\\filtro.csv'
periodo_file = 'src\\entities\\datasets\\periodo.csv'
consulta_venda_file = 'src\\entities\\datasets\\consulta_venda.csv'

df_bebida = pd.read_csv(bebida_file, delimiter=';', quoting=3, encoding='utf-8', encoding_errors='ignore')
df_usuario = pd.read_csv(usuario_file, delimiter=';', quoting=3, encoding='utf-8', encoding_errors='ignore')
df_consulta = pd.read_csv(consulta_file, delimiter=';', quoting=3, encoding='utf-8', encoding_errors='ignore')
df_venda = pd.read_csv(venda_file, delimiter=';', quoting=3, encoding='utf-8', encoding_errors='ignore')
df_filtro = pd.read_csv(filtro_file, delimiter=';', quoting=3, encoding='utf-8', encoding_errors='ignore')
df_periodo = pd.read_csv(periodo_file, delimiter=';', quoting=3, encoding='utf-8', encoding_errors='ignore')
df_consulta_venda = pd.read_csv(consulta_venda_file, delimiter=';', quoting=3, encoding='utf-8', encoding_errors='ignore')

df_venda['DATAVENDA'] = pd.to_datetime(df_venda['DATAVENDA'])
df_venda['VOLUMEVENDIDO'] = pd.to_numeric(df_venda['VOLUMEVENDIDO'])

print(df_bebida)
print(df_usuario)
print(df_consulta)
print(df_venda)
print(df_filtro)
print(df_periodo)

df_join = df_venda.join(df_bebida, on='ID_BEBIDA', how='inner', lsuffix='_VENDA', rsuffix='_BEBIDA')

print(df_join)

df_join = df_join[(df_join['DATAVENDA'].dt.year >= 2012) & (df_join['DATAVENDA'].dt.year <= 2016)]

print(df_join)

df = df_join[(df_join['CATEGORIA'] == "IMPORTED VODKAS")]

print(df)

soma = df['VOLUMEVENDIDO'].sum()

print(soma)

df_relatorio = df_consulta.set_index('IDCONSULTA').join(df_filtro.set_index('ID_CONSULTA'), how='inner')
df_relatorio = df_periodo.set_index('ID_CONSULTA').join(df_relatorio.set_index('IDCONSULTA'), how='inner')
df_relatorio = df_usuario.set_index('IDUSUARIO').join(df_relatorio.set_index('ID_USUARIO'), how='inner')
df_relatorio = df_consulta_venda.set_index('ID_CONSULTA').join(df_relatorio.set_index('IDCONSULTA'), how='inner')
df_relatorio = df_relatorio.set_index('ID_VENDA').join(df_join.set_index('IDVENDA'), how='inner')

print(df_relatorio)
