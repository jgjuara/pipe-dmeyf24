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
import lgbm_globales
import funciones_lgbm
from datetime import datetime

# init time
start_time = time()

# mes test
nombre_mes_test = "mayo"

#%%

X_train, y_train_binaria1, y_train_binaria2, w_train, X_test, y_test_class, y_test_binaria1, w_test = funciones_lgbm.preparar_data(lgbm_globales.dataset_path, mes_train= lgbm_globales.mes_train, mes_test= lgbm_globales.mes_test, sampling= 1)

#%%

study = optuna.create_study(
    direction="maximize",
    study_name=lgbm_globales.study_name,
    storage=lgbm_globales.storage_name,
    load_if_exists=True,
)


path_modelos = f'{lgbm_globales.exp_path}/modelos/{lgbm_globales.study_name}/'

path_csv = f'{lgbm_globales.exp_path}/csv/{lgbm_globales.study_name}/'

print(f'Path: {path_modelos}')
print(f'Path: {path_csv}')

# create directory for saving models
if not os.path.exists(path_modelos):
    os.makedirs(path_modelos)

    # create directory for saving models
if not os.path.exists(path_csv):
    os.makedirs(path_csv)

#%% write log file with study name and time of start
with open(path_modelos + f'/log_{start_time}.txt', 'w') as f:
    f.write(f'Study: {lgbm_globales.study_name}\n')

with open(path_modelos + f'/log_{start_time}.txt', 'a') as f:
    f.write(f'Start time: {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}\n')

with open(path_modelos + f'/log_{start_time}.txt', 'a') as f:
    f.write(f'Test contra mes: {nombre_mes_test}\n')

#%%

train_data = lgb.Dataset(X_train,
                        label=y_train_binaria2,
                        weight=w_train)

if os.path.exists('testing/train_data.bin'):
    print("Archivo train_data.bin ya existe. Borrando...")
    os.remove('testing/train_data.bin')

train_data.save_binary('testing/train_data.bin')

print("Archivo train_data.bin guardado")

def backtesting_lgbm():
  
    top_n = study.trials_dataframe().sort_values(by="value", ascending=False).iloc[:lgbm_globales.top_n]['number'].tolist()

    print("Testing top trials:", lgbm_globales.top_n)

    # log trials to file
    study.trials_dataframe().sort_values(by="value", ascending=False).iloc[:lgbm_globales.top_n].to_csv(path_csv + f'top_n_trials_{start_time}.csv', index=False)

    for i in top_n:

        trial_params = study.trials[i].params

        print("Testing trial:", i, "params:", trial_params)

        with open(path_modelos + f'/log_{start_time}.txt', 'a') as f:
            f.write(f'Testing trial: {i} \n "params: {trial_params}\n')
        
        best_iter = study.trials[i].user_attrs['best_iter']

        with open(path_modelos + f'/log_{start_time}.txt', 'a') as f:
            f.write(f'Best iter: {best_iter} \n')
        
        for semilla in lgbm_globales.semillas:

            df_path = path_csv + 'df_cut_point-{study}-{trial}-{semilla}-{nombre_mes_test}.csv'.format(study = lgbm_globales.study_name, trial = i, semilla = semilla)

            if os.path.exists(df_path):
                print("Archivo testing ya existe: "+'df_cut_point-{study}-{trial}-{semilla}-{nombre_mes_test}.csv'.format(study = lgbm_globales.study_name, trial = i, semilla = semilla))
                
                
                with open(path_modelos + f'/log_{start_time}.txt', 'a') as f:
                    f.write(f"{datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M:%S')} Archivo testing ya existe: "+'df_cut_point-{study}-{trial}-{semilla}-{nombre_mes_test}.csv\n'.format(study = lgbm_globales.study_name, trial = i, semilla = semilla))
                
                continue

            ganancia = np.where(y_test_binaria1 == 1, lgbm_globales.ganancia_acierto, 0) - np.where(y_test_binaria1 == 0, lgbm_globales.costo_estimulo, 0)

            var_params = {'seed': semilla}

            params = {**lgbm_globales.fixed_params, **trial_params, **var_params}

            print(f"Entrenando con semilla {semilla}")


            model_path = path_modelos + "model-{study}-{trial}-{semilla}-{nombre_mes_test}.pkl".format(study = lgbm_globales.study_name, trial = i, semilla = semilla)

            train_data = lgb.Dataset("testing/train_data.bin")

            # format time to log file

            if os.path.exists(model_path):
                
                print(f"Archivo {model_path} ya existe. Loading...")

                with open(path_modelos + f'/log_{start_time}.txt', 'a') as f:
                    f.write(f"{datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M:%S')} - Archivo {model_path} ya existe. Loading...\n")
      
                # load model
                with open(model_path, 'rb') as f:
                    model = pickle.load(f)
            else:

                start_train_time = datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M:%S')

                with open(path_modelos + f'/log_{start_time}.txt', 'a') as f:

                    f.write(f'Start training seed {semilla}: {start_train_time}\n')

                model = lgb.train(params,
                                train_data,
                                num_boost_round=best_iter)
                
                end_train_time = datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M:%S')

                with open(path_modelos + f'/log_{start_time}.txt', 'a') as f:
                    f.write(f'End training seed {semilla}: {end_train_time}\n')
            
                # Save the model using pickle
                with open(model_path, 'wb') as f:
                    pickle.dump(model, f)

                with open(path_modelos + f'/log_{start_time}.txt', 'a') as f:
                    f.write(f'Modelo guardado: {model_path}\n')

                print("Modelo guardado: "+model_path)

                print("Train terminado")

            y_pred_lgm = model.predict(X_test)

            idx = np.argsort(y_pred_lgm)[::-1]

            ganancia = ganancia[idx]
            y_pred_lgm = y_pred_lgm[idx]

            df_cut_point = pd.DataFrame({'ganancia': ganancia, 'y_pred_lgm': y_pred_lgm})

            private_idx, public_idx = train_test_split(df_cut_point.index, test_size=0.3, random_state=semilla, stratify=y_test_binaria1)

            df_cut_point['public'] = 0.0
            df_cut_point['private'] = 0.0
            df_cut_point.loc[private_idx, 'private'] = ganancia[private_idx] / 0.7
            df_cut_point.loc[public_idx, 'public'] = ganancia[public_idx] / 0.3

            df_cut_point['nro_envios'] = df_cut_point.reset_index().index

            df_cut_point['public_cum'] = df_cut_point['public'].cumsum()
            df_cut_point['private_cum'] = df_cut_point['private'].cumsum()

            df_cut_point.to_csv(df_path, index=False)

            print("Archivo testing guardado: "+'df_cut_point-{study}-{trial}-{semilla}.csv'.format(study = lgbm_globales.study_name, trial = i, semilla = semilla))

#%%
backtesting_lgbm()

