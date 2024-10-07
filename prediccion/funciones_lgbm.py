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




#%%


def preparar_data(dataset_path, dataset_file, mes_train, mes_test, drop_cols="lag2|variacion2"):

    print('Preparando data...')

    dataset = ds.dataset(dataset_path, format="parquet", partitioning="hive")

    columnas = dataset.schema.names

    cols_selected  = [col for col in columnas if not pd.Series(col).str.contains(drop_cols, regex=True).any()]

    data = dataset.to_table(columns=cols_selected).to_pandas()

    # data = pd.read_parquet(dataset_path + dataset_file)

    data['clase_peso'] = 1.0

    data.loc[data['clase_ternaria'] == 'BAJA+2', 'clase_peso'] = 1.00002
    data.loc[data['clase_ternaria'] == 'BAJA+1', 'clase_peso'] = 1.00001

    data['clase_binaria1'] = 0
    data['clase_binaria2'] = 0
    data['clase_binaria1'] = np.where(data['clase_ternaria'] == 'BAJA+2', 1, 0)
    data['clase_binaria2'] = np.where(data['clase_ternaria'] == 'CONTINUA', 0, 1)

    train_data = data.loc[data['foto_mes'].isin(mes_train)]
    test_data = data[data['foto_mes'] == mes_test]

    X_train = train_data.drop(['clase_ternaria', 'clase_peso', 'clase_binaria1','clase_binaria2'], axis=1)
    y_train_binaria1 = train_data['clase_binaria1']
    y_train_binaria2 = train_data['clase_binaria2']
    w_train = train_data['clase_peso']

    X_test = test_data.drop(['clase_ternaria', 'clase_peso', 'clase_binaria1','clase_binaria2'], axis=1)
    y_test_binaria1 = test_data['clase_binaria1']
    y_test_class = test_data['clase_ternaria']
    w_test = test_data['clase_peso']

    print('Data preparada')

    return X_train, y_train_binaria1, y_train_binaria2, w_train, X_test, y_test_class, y_test_binaria1, w_test


