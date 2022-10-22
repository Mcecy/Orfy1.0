def filtros(qtd_filtros):
    filtros_extra = ""
    filtro_list = []

    for i in range(1, qtd_filtros + 1):

        tipo_filtro = input(f"Qual o tipo do filtro {i}?(NOMEPRODUTO/CLASSETERAPEUTICA/PRINCIPIOATIVO) ")

        conteudo_filtro = input(f"Qual o conteúdo do filtro {i}? ")

        filtro = f"{tipo_filtro},{conteudo_filtro};"

        filtro_str = f"M.{tipo_filtro} LIKE '%{conteudo_filtro}%',"

        filtros_extra += filtro_str

        filtro_list.append(filtro)

    return filtros_extra, filtro_list


def filtros_list(filtro_list):
    tipos_filtro = []
    conteudos_filtro = []
    for filtro in filtro_list:
        tipo_filtro, conteudo_filtro = filtro.split(",")
        tipos_filtro.append(tipo_filtro)
        conteudos_filtro.append(conteudo_filtro)

    return tipos_filtro, conteudos_filtro


def orderby(order, tipos_filtro):
    order_by = ""
    for tipo_filtro in tipos_filtro:
        if order == tipo_filtro:
            order_by = order    # Mostra a coluna usada para o order by
    return order_by
