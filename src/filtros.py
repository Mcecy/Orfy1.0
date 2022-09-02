def filtros(filtros_extra, tipo_filtro, conteudo_filtro):
    filtro_str = f"M.{tipo_filtro} LIKE '%{conteudo_filtro}%',"

    filtros_extra += filtro_str


def filtrolist(view_filtro_list, filtro, tipo_filtro, conteudo_filtro):
    filtro = f"{tipo_filtro},{conteudo_filtro};"

    view_filtro_list.append(filtro)


def orderby(order, tipos_filtro):
    order_by = ""
    for tipo_filtro in tipos_filtro:
        if order == tipo_filtro:
            order_by = order    # Mostra a coluna usada para o order by
    return order_by
