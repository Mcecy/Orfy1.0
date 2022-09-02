
"""
 # Escolhendo a coluna do order by
order = input("Qual coluna gostaria de usar para ordenar a tabela?(NOME_PRODUTO, CLASSE_TERAPEUTICA, PRINCIPIO_ATIVO) ")
order_by = ""
for tipo_filtro in tipos_filtro:
    if order == tipo_filtro:
        order_by = order    # Mostra a coluna usada para o order by
                        """
import filtros

order = '2'
tipos_filtro = ['1', '2', '3']

order_by = filtros.orderby(order, tipos_filtro)
print(order_by)
