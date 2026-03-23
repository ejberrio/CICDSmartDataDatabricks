import pandas as pd
import re

pop = pd.read_csv('datasets/Electric_Vehicle_Population_Data.csv')
spc = pd.read_csv('datasets/electric_vehicles_spec_2025.csv.csv')

def normalize(s):
    if pd.isna(s): return s
    return re.sub(r'\s+', ' ', str(s).strip()).upper()

pop['join_make'] = pop['Make'].apply(normalize)
pop['join_model'] = pop['Model'].apply(normalize)
spc['join_make'] = spc['brand'].apply(normalize)
spc['join_model'] = spc['model'].apply(normalize)

pop_distinct = pop[['join_make','join_model']].drop_duplicates()

matches = []
for _, row in pop_distinct.iterrows():
    pm = row['join_make']
    pmodel = row['join_model']
    matched = spc[(spc['join_make'] == pm) & (spc['join_model'].str.startswith(pmodel, na=False))]
    if not matched.empty:
        matches.append({
            'join_make': pm,
            'join_model': pmodel,
            'n_spec_matches': len(matched),
            'avg_range_km': matched['range_km'].mean()
        })

df_matches = pd.DataFrame(matches)
print(f'Total pop combos: {len(pop_distinct)}, matched: {len(df_matches)}')
print()
print('Matches by make:')
summary = df_matches.groupby('join_make').size().reset_index(name='model_combos_matched')
print(summary.sort_values('model_combos_matched', ascending=False).to_string())
print()
brands = ['TESLA','MINI','BMW','KIA','PORSCHE','SUBARU','VOLVO','HYUNDAI','FORD']
for brand in brands:
    sub = df_matches[df_matches['join_make'] == brand]
    if not sub.empty:
        models = sub['join_model'].tolist()
        print(f'{brand}: {models}')
    else:
        print(f'{brand}: NO MATCH')
