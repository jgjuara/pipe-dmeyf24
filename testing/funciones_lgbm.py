#%%
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.model_selection import ShuffleSplit, StratifiedShuffleSplit
from sklearn.ensemble import RandomForestClassifier
import lightgbm as lgb
import optuna
from optuna.visualization import plot_optimization_history, plot_param_importances, plot_slice, plot_contour
from time import time
import pickle
import os
from dotenv import load_dotenv
import urllib
import pyarrow.dataset as ds
import lgbm_globales
import duckdb
from pathlib import Path




#%%


def preparar_data(dbname, mes_train, mes_test, drop_cols=None, sampling = 1):

    print('Preparando data...')

    root_path = Path(__file__).parent.parent.resolve()

    root_path = root_path.as_posix() + '/'

    # Connect to DuckDB database
    con = duckdb.connect(root_path+'duckdb/'+ dbname + ".duckdb")       

    meses = mes_train + mes_test
    
    meses = ', '.join(map(str, meses))

    
    query = (f"""
            SELECT *
            FROM {dbname}
            JOIN {dbname}_lags
            ON {dbname}.foto_mes = {dbname}_lags.foto_mes
            AND {dbname}.numero_de_cliente = {dbname}_lags.numero_de_cliente
            AND {dbname}.clase_ternaria = {dbname}_lags.clase_ternaria
            WHERE {dbname}.foto_mes IN ({meses});""")

    # Execute the join query and fetch the result as a Pandas DataFrame
    data = con.execute(query).fetchdf()

    con.close()

    data = data.drop(['numero_de_cliente_1', 'foto_mes_1', 'clase_ternaria_1'], axis=1)

    # data = pd.read_parquet(dataset_path + dataset_file)

    data['clase_peso'] = 1.0

    data.loc[data['clase_ternaria'] == 'BAJA+2', 'clase_peso'] = 1.00002
    data.loc[data['clase_ternaria'] == 'BAJA+1', 'clase_peso'] = 1.00001

    data['clase_binaria1'] = 0
    data['clase_binaria2'] = 0
    data['clase_binaria1'] = np.where(data['clase_ternaria'] == 'BAJA+2', 1, 0)
    data['clase_binaria2'] = np.where(data['clase_ternaria'] == 'CONTINUA', 0, 1)

#   data = data.loc[data['foto_mes'].isin(mes_train + mes_test)]

    data_continua = data[data['clase_ternaria'] == 'CONTINUA']

    data_continua = data_continua.groupby('foto_mes').sample(frac=sampling, random_state=42)

    data = pd.concat([data_continua, data[data['clase_ternaria'] != 'CONTINUA']])

    print(data.shape)

    print(data.head())

    del data_continua

    train_data = data.loc[data['foto_mes'].isin(mes_train)]
    test_data = data[data['foto_mes'].isin(mes_test)]

    X_train = train_data.drop(['numero_de_cliente','clase_ternaria', 'clase_peso', 'clase_binaria1','clase_binaria2'], axis=1)
    y_train_binaria1 = train_data['clase_binaria1']
    y_train_binaria2 = train_data['clase_binaria2']
    w_train = train_data['clase_peso']

    X_test = test_data.drop(['clase_ternaria', 'clase_peso', 'clase_binaria1','clase_binaria2'], axis=1)
    y_test_binaria1 = test_data['clase_binaria1']
    y_test_class = test_data['clase_ternaria']
    w_test = test_data['clase_peso']

    print('Data preparada')

    return X_train, y_train_binaria1, y_train_binaria2, w_train, X_test, y_test_class, y_test_binaria1, w_test


