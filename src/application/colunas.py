def col(cols):
    colunas = ["CLASSETERAPEUTICA", "PRINCIPIOATIVO", "NOMEPRODUTO"]
    colunas_filtradas = []

    for i in range(1, len(cols) + 1):
        for coluna in colunas:
            for coluna_escolhida in cols:
                if coluna_escolhida == coluna:
                    colunas_filtradas.append(coluna_escolhida)
                else:
                    print(f"Coluna {coluna_escolhida} não existe.")

    colunas_filtradas = ", ".join(colunas_filtradas)

    return colunas_filtradas
