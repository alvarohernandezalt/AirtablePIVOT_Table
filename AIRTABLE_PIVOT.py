import airtable
import pandas as pd

# Connect to Airtable API
airtable_api_key = 'AIRTABLE_KEY'
base_key = 'BASE_ID'
oportunidades_table_name = 'Oportunidades'
valores_pivot_table_name = 'Valores_PIVOT'
conn = airtable.Airtable(base_key, oportunidades_table_name, api_key=airtable_api_key)

# Get all data as a dataframe
records = conn.get_all()
df = pd.DataFrame.from_records([r['fields'] for r in records])

# Pivot table with Estado as rows and Prioridad as columns
pt = pd.pivot_table(df, 
                    values='Presupuesto_VAL', 
                    index='Estado', 
                    columns='Prioridad', 
                    aggfunc='sum')

# Insert pivot table values into Airtable
valores_pivot_conn = airtable.Airtable(base_key, valores_pivot_table_name, api_key=airtable_api_key)

for estado in pt.index:
    for prioridad in pt.columns:
        presupuesto_val = pt.loc[estado, prioridad]
        record = {'Estado': estado, 'Prioridad': prioridad, 'Presupuesto_VAL': presupuesto_val}
        valores_pivot_conn.insert(record)