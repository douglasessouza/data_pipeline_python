

from data_processing import Data

#Defining where the data are
path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'

#EXTRACT
data_companyA = Data(path_json, 'json')
print(f'Column name for company A is: {data_companyA.column_names}\n')
print(f'Number of line for the company A file is: {data_companyA.number_lines}\n')

data_companyB = Data(path_csv, 'csv')
print(f'Column name for company B is: {data_companyB.column_names}\n')
print(f'Number of line for the company B file is: {data_companyB.number_lines}\n')

#DATA TRANSFORMATION
#Mapping all column name
key_mapping = {'Nome do Item' : 'Nome do Produto',
               'Classificação do Produto' : 'Categoria do Produto',
               'Valor em Reais (R$)' : 'Preço do Produto (R$)',
               'Quantidade em Estoque': 'Quantidade em Estoque',
               'Nome da Loja' : 'Filial',
               'Data da Venda' : 'Data da Venda'}

data_companyB.rename_columns(key_mapping)
print(f'Columns name after the renaming - The renaming is being done to standardize {data_companyB.column_names}\n')

fusion_data = Data.join(data_companyA, data_companyB)
print(f'Column name after the JOIN is: {fusion_data.column_names}\n')
print(f'Number of line fafter the JOIN: {fusion_data.number_lines}\n')


#LOADING
path_combined_data = 'data_processed/combined_data.csv'
fusion_data.saving_data(path_combined_data)
print(path_combined_data)
