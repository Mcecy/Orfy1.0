# pylint: disable=C0103,C0114,C0115,C0116, C0209, C0301, W0105
def filtros(filtros_extra, tipo_filtro, conteudo_filtro):
    filtro_str = f"M.{tipo_filtro} LIKE '%{conteudo_filtro}%',"

    filtros_extra += filtro_str

    return filtros_extra


def filtrolist(view_filtro_list, filtro, tipo_filtro, conteudo_filtro):
    filtro = f"{tipo_filtro},{conteudo_filtro};"

    view_filtro_list.append(filtro)

    return view_filtro_list
