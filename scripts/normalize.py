import pandas as pd

df = pd.read_csv('../data/input/frota_detran_por_uf_2025.csv', sep=';')
df.head()

df["Ano Fabricação Veículo CRV"] = pd.to_numeric(
    df["Ano Fabricação Veículo CRV"], errors="coerce"
)

df["Qtd. Veículos"] = pd.to_numeric(
    df["Qtd. Veículos"], errors="coerce"
)

df = df.dropna(subset=["Ano Fabricação Veículo CRV", "Qtd. Veículos"])

df["Ano Fabricação Veículo CRV"] = df["Ano Fabricação Veículo CRV"].astype(int)
df["Qtd. Veículos"] = df["Qtd. Veículos"].astype(int)

df["qtd_barras"] = df["Marca Modelo"].str.count("/")

df["IMPORTADO"] = df["Marca Modelo"].str.startswith("I/", na=False)

df["Marca Modelo s/ importado"] = df["Marca Modelo"].str.removeprefix("I/")

df["qtd_barras"] = df["Marca Modelo"].str.count("/")

df.loc[df["qtd_barras"] >= 1, "MARCA"] = (
    df.loc[df["qtd_barras"] >= 1, "Marca Modelo s/ importado"]
      .str.split("/")
      .str[0]
)

df["Marca Modelo s/ marca"] = (
    df["Marca Modelo s/ importado"]
      .str.replace(r"^[^/]+/", "", regex=True)
)

df['ARTESANAL'] = df['MARCA'].str.startswith('A.', na=False)

resumo = df[(~df['ARTESANAL']) & (~df['IMPORTADO'])].groupby("MARCA")["Qtd. Veículos"].sum()
marcas_validas = resumo[resumo > 10]

df_final = df[df['MARCA'].isin(marcas_validas.index)]

df_final = df_final.dropna(subset=['MARCA'])

columns = ['UF', 'MARCA', 'Marca Modelo', 'Marca Modelo s/ marca', 'Ano Fabricação Veículo CRV', 'Qtd. Veículos']

columns_map = {
    'UF': 'uf',
    'MARCA': 'marca',	
    'Marca Modelo s/ marca': 'modelo',
    'Ano Fabricação Veículo CRV': 'ano_fabricacao',
    'Qtd. Veículos': 'frota',
    'Marca Modelo': 'descricao_detran',
}

df_final = df_final[columns].rename(columns=columns_map)

# LOAD df_final as detran_vehicle_norm