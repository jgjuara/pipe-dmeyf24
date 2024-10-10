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
import pickle

X_train, y_train_binaria1, y_train_binaria2, w_train, X_test, y_test_class, y_test_binaria1, w_test = funciones_lgbm.preparar_data(lgbm_globales.dataset_path, lgbm_globales.dataset_file, mes_train = [202104], mes_test = 202106)


study = optuna.create_study(
    direction="maximize",
    study_name=lgbm_globales.study_name,
    storage=lgbm_globales.storage_name,
    load_if_exists=True,
)


def predecir(n_top=5):

    top_trials = study.trials_dataframe().sort_values(by="value", ascending=False).iloc[:n_top]['number'].tolist()

    print('Top 5 trials: ', top_trials)

    for i in top_trials:

        print(f'Trial: {i}')

        trial_params = study.trials[i].params

        print('Params: ', trial_params)
        
        best_iter = study.trials[i].user_attrs['best_iter']
        
        for semilla in lgbm_globales.semillas:

            var_params = {'seed': semilla}

            params = {**lgbm_globales.fixed_params, **trial_params, **var_params}

            train_data = lgb.Dataset(X_train,
                                    label=y_train_binaria2,
                                    weight=w_train)
            
            print(f'Entrenando modelo con semilla: {semilla}')

            model = lgb.train(params,
                            train_data,
                            num_boost_round=best_iter)

            print('Entrenamiento finalizado')

            # Path to save the pickle file
            path_modelo_file = 'lgbm-{study}-{trial}-{semilla}-train-04.txt'.format(study = lgbm_globales.study_name, trial = i, semilla = semilla)

            # Save the model using pickle
            with open(lgbm_globales.modelos_path+path_modelo_file, 'wb') as f:
                pickle.dump(model, f)
            
            print(f'Modelo guardado en {path_modelo_file}')

            y_pred_lgm = model.predict(X_test)

            rango = np.arange(lgbm_globales.min_envios, lgbm_globales.max_envios, lgbm_globales.paso_envios)

            for i in rango:
                data_export = X_test['numero_de_cliente'].to_frame()
                data_export['Prob'] = y_pred_lgm
                data_export = data_export.sort_values(by='Prob', ascending=False)
                data_export['Predicted'] = 0
                data_export.iloc[:i, data_export.columns.get_loc('Predicted')] = 1
                envios = data_export['Predicted'].sum()
                data_export = data_export[['numero_de_cliente', 'Predicted']]
                data_export['numero_de_cliente'] = data_export['numero_de_cliente'].astype(int)
                envio_path = 'envios/' + 'lgbm-{study}-{trial}-{envios}-{semilla}.csv'.format(study = lgbm_globales.study_name, trial = i, envios = envios, semilla = semilla)
                data_export.to_csv(envio_path, index=False)
                # Define the command
                mensaje = 'Lag t-1, ranks, variaciones train abr - study {study} trial {trial} semilla {semilla} - envios {envios}'.format(semilla = semilla, envios = envios, study = lgbm_globales.study_name, trial = study.best_trial.number)

                command = 'kaggle competitions submit -c dm-ey-f-2024-primera -f {envio_path} -m "{mensaje}"'.format(envio_path = envio_path, mensaje = mensaje)
                print(mensaje)
                # Execute the command
                os.system(command)




# prediccion
predecir()
