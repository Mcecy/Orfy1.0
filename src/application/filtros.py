def filtros(qtd_filtros):
    filtro_list = []

    for i in range(1, qtd_filtros + 1):

        tipo_filtro = input(f"Qual o tipo do filtro {i}?(CATEGORIA/NOMEPRODUTO) ")
        conteudo_filtro = input(f"Qual o conteúdo do filtro {i}? ")

        filtro = f"{tipo_filtro},{conteudo_filtro};"

        filtro_list.append(filtro)

    return filtro_list


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
