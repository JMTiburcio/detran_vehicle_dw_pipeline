import pandas as pd

df = pd.read_csv('data/input/I_Frota_por_UF_Municipio_Marca_e_Modelo_Ano_2025.txt', sep=';')
df = (
    df
    .drop(columns=['Município'])  # Remove a coluna
    .groupby(['UF', 'Marca Modelo', 'Ano Fabricação Veículo CRV'], as_index=False)
    ['Qtd. Veículos']
    .sum()
)
df.to_csv('data/input/frota_detran_por_uf_2025.csv', sep=';', index=False)

