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
import lgbm_globales
import funciones_lgbm
import gc




X_train, y_train_binaria1, y_train_binaria2, w_train, X_test, y_test_class, y_test_binaria1, w_test = funciones_lgbm.preparar_data(
                                                                                                                    dbname=lgbm_globales.dataset_path,
                                                                                                                    mes_train= lgbm_globales.mes_train,
                                                                                                                    mes_test= lgbm_globales.mes_test,
                                                                                                                    sampling= lgbm_globales.sampling)

# filas por mes en X_train
n_train_rows = X_train.shape[0]/len(lgbm_globales.mes_train +  lgbm_globales.mes_test)

# cols selection
len(X_train.columns)
# monotone restrictions

# polinomials combinations



def lgb_gan_eval(y_pred, data):
    weight = data.get_weight()
    ganancia = np.where(weight == 1.00002, lgbm_globales.ganancia_acierto, 0) - np.where(weight < 1.00002, lgbm_globales.costo_estimulo, 0)
    ganancia = ganancia[np.argsort(y_pred)[::-1]]
    ganancia = np.cumsum(ganancia)

    return 'gan_eval', np.max(ganancia) , True



def objective(trial):


    train_data = lgb.Dataset(data = f"train_data_{lgbm_globales.study_name}.bin")

    p_min_data_in_leaf = trial.suggest_float('p_min_data_in_leaf', 0.0001, 0.002)

    params_objetivo = {
    'num_leaves' : trial.suggest_int('num_leaves', 500, 100000),
    'learning_rate' : trial.suggest_float('learning_rate', 0.001, 0.05), # mas bajo, más iteraciones necesita
    # 'min_data_in_leaf' : trial.suggest_int('min_data_in_leaf', 50, 8000),
    'min_data_in_leaf' : int(p_min_data_in_leaf * n_train_rows),
    # 'n_estimators': trial.suggest_int('n_estimators', 10000, 100000),
    'feature_fraction' : trial.suggest_float('feature_fraction', 0.3, .9),
    'feature_fraction_bynode' : trial.suggest_float('feature_fraction_bynode', 0.3, .9), 
    'drop_rate': trial.suggest_float('drop_rate', 0.005, 0.3),
    'min_split_gain': 1,
    }

    semilla = np.random.choice(lgbm_globales.semillas)

    params_objetivo['seed'] = semilla

    params = {**lgbm_globales.fixed_params, **params_objetivo}

    print("###########################################################")
    print(f"Trial Params: {params}")

    learning_rate = params['learning_rate']

    cv_results = lgb.cv(
        params,
        train_data,
        num_boost_round= 10000, # modificar, subit y subir... y descomentar la línea inferior
        callbacks=[lgb.early_stopping(stopping_rounds= int(200 + 5 / learning_rate ))],
        feval=lgb_gan_eval,
        stratified=True,
        nfold=5
    )

    max_gan = max(cv_results['valid gan_eval-mean'])
    best_iter = cv_results['valid gan_eval-mean'].index(max_gan) + 1

    # Guardamos cual es la mejor iteración del modelo
    trial.set_user_attr("best_iter", best_iter)
    trial.set_user_attr("train_months", lgbm_globales.mes_train)
    trial.set_user_attr("seed", int(semilla))
    trial.set_user_attr("sampling", int(lgbm_globales.sampling))



    return max_gan * 5 / len(lgbm_globales.mes_train)


study = optuna.create_study(
    direction="maximize",
    study_name=lgbm_globales.study_name,
    storage=lgbm_globales.storage_name,
    load_if_exists=True
)


if not os.path.exists(f"train_data_{lgbm_globales.study_name}.bin"):
    train_data = lgb.Dataset(X_train,
                            label=y_train_binaria2, # eligir la clase
                            weight=w_train,
                            params = {
                            'max_bin' : 255,
                            'feature_pre_filter': True,
                            'seed' : 42
                            }
                           )

    train_data.save_binary(f"train_data_{lgbm_globales.study_name}.bin")

del X_train, X_test

gc.collect()

if lgbm_globales.optimizar:
    study.optimize(objective, n_trials= lgbm_globales.intentos, gc_after_trial=True) # subir subir

