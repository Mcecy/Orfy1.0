# pylint: disable=C0103,C0114,C0115,C0116, C0209, C0301, W0105
"""
            # Começo do bloco de instanciar tabelas do banco de dados
            table_consulta_query = f"SELECT * FROM {table_consulta_nome}"
            table_medicamento_query = f"SELECT * FROM {table_medicamento_nome}"
            table_filtro_query = f"SELECT * FROM {table_filtro_nome}"
            table_periodo_query = f"SELECT * FROM {table_periodo_nome}"
            # Fim do bloco de instanciar tabelas do banco de dados

            table_venda_query = Acesso em loop para escolher a table_venda[i] através do período selecionado
            para mesclar num só dataset e resultar nessa variável

#Começo do bloco para seleção de datasets para table_venda_query
periodos = (int(input("Quantos períodos gostaria de incluir na consulta? ")))
datasets = []
for i in range(1, periodos+1):
    mes = input(f"Qual o mês do período {i}? ")
    ano = input(f"Qual o ano do período {i}? ")
    periodo = f"{mes}/{ano}.csv"
    pasta = "./entities/datasets"
    for diretorio, subpastas, arquivos in os.walk(pasta):
        for arquivo in arquivos:
            arquivo = os.path.join(diretorio, arquivo)
            if arquivo.endswith(periodo):
                datasets = datasets.append(arquivo)
print(datasets)

counter = 0
while counter < len(datasets):

# Fim do bloco de seleção de datasets para table_venda_query



            #Começo do bloco para configurar consultas
            consulta_quest = input("Gostaria de iniciar uma nova consulta?(s/n)")

            if consulta_quest == "s":

            else:
                # Começo do bloco para view filtrada
                view_filtro_nome = input("Como gostaria de nomear a tabela filtrada de medicamentos? ")

                view_query_list_tipo = []
                view_query_list_conteudo = []

                for filtro in view_filtro_list:
                    tipo_filtro, conteudo_filtro = filtro.split(",")
                    view_query_list_tipo = f"{tipo_filtro}, {conteudo_filtro}"

                # Começo do bloco para colunas filtradas
                colunas_escolhidas_visual = list(input("Quais colunas gostaria de ver?(CLASSE_TERAPEUTICA, PRINCIPIO_ATIVO, NOME_PRODUTO) "))
                colunas = [CLASSE_TERAPEUTICA, PRINCIPIO_ATIVO, NOME_PRODUTO]
                colunas_filtradas = len(colunas_escolhidas_visual) * []

                for i in enumerate(colunas_filtradas):
                    for coluna in colunas:
                        if coluna == colunas[i]:
                            colunas_filtradas = colunas_filtradas.append(coluna)
                        else:
                            continue
                colunas_filtradas = ", ".join(colunas_filtradas)

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
                filtros_extra = "OR ".join(filtros_extra)

                tipos_filtro = []
                for filtro in view_filtro_list:
                    tipo_filtro, conteudo_filtro = filtro.split(",")
                    tipos_filtro = tipos_filtro.append(tipo_filtro)

                order = input("Qual coluna gostaria de usar para ordenar a tabela?(NOME_PRODUTO, CLASSE_TERAPEUTICA, PRINCIPIO_ATIVO) ")
                order_by = ""
                for tipo_filtro in tipos_filtro:
                    if order == tipo_filtro:
                        order_by = order



                #Fim do bloco para colunas filtradas


                view_filtro_query = f"CREATE TABLE IF NOT EXISTS {view_filtro_nome} AS SELECT {colunas_filtradas} FROM {table_medicamento_nome} WHERE {filtros_extra} ORDER BY {order_by}"
                # Fim do bloco para view filtrada

                view_soma_nome = input("Como gostaria de nomear a visualização do resultado? ")

                view_soma[i] = f"CREATE VIEW {view_soma_nome} AS SELECT ANO_VENDA, SUM(QTD_VENDIDA) FROM {table_venda_nome} INNER JOIN {view_filtro_nome} ON {view_filtro_nome}.PRINCIPIO_ATIVO={table_venda_nome}.PRINCIPIO_ATIVO WHERE {table2}.{tipo_filtro} LIKE '{conteudo_filtro}%' {filtros_extra} AND {table1}.PRINCIPIO_ATIVO IS NOT NULL AND {table2}.PRINCIPIO_ATIVO IS NOT NULL"
                cursor.execute(view_consulta[i])

                # Fim do bloco para configurar consultas

            """
