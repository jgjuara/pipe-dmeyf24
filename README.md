# pipe-dmeyf24

# Notas

### 10/10/2024
Entrenando con todas las variables (las q vienen + ranks + lag1) sobre marzo a abril da mejor resultado al parecer que solo entrenar sobre abril. Cambia un poco las variables a las que se da importancia, en el primer caso ctrx y total patrimonio las 2 mas importantes. No se ven variables rank en el top 100 vars lo que me parece raro. Habría que explorar tmb otros hiperparam para explorar.

Probaria evaluar variables sucias y si removerlas.
Probar calcula el PSI variable contra variable abr vs junio marz vs junio y abr+marz vs junio
Probaría quitando las variables numericas que no tienen rank.
Probaría tmb entrenar solo sobre abril con lags t-2
Probaría por ultimo entrenar sobre abril y marzo con lags t-2

### 3/11/2024

-------
Hice optimizacion y backtesting sin sampling con las siguientes definiciones:

fixed_params = {
    'objective': 'binary',
    'metric': 'custom',
    'boosting_type': 'gbdt',
    'first_metric_only': True,
    'boost_from_average': True,
    'feature_pre_filter': False,
    'force_col_wise' : True,
    'verbose': -1}

params_objetivo = {
    'num_leaves' : trial.suggest_int('num_leaves', 20, 10000),
    'learning_rate' : trial.suggest_float('learning_rate', 0.001, 0.4), # mas bajo, más iteraciones necesita
    'min_data_in_leaf' : int(p_min_data_in_leaf*n_train_rows),
    'feature_fraction' : trial.suggest_float('feature_fraction', 0.005, .9),
    'feature_fraction_bynode' : trial.suggest_float('feature_fraction_bynode', 0.05, .9), 
    'drop_rate': trial.suggest_float('drop_rate', 0.005, 0.3),
    'min_split_gain': trial.suggest_int('min_split_gain', 1, 40000)
    }

Con 200 vueltas y entrenando sobre el periodo 202011-202104

Top 3 de modelos resultaron ser

[{'p_min_data_in_leaf': 0.0077293493962832145,
  'num_leaves': 3124,
  'learning_rate': 0.01836059829452097,
  'feature_fraction': 0.3776329076614978,
  'feature_fraction_bynode': 0.49105821717349096,
  'drop_rate': 0.2292801154535743,
  'min_split_gain': 1},
 {'p_min_data_in_leaf': 0.030951759327442402,
  'num_leaves': 3031,
  'learning_rate': 0.07413995701955478,
  'feature_fraction': 0.3504410891347088,
  'feature_fraction_bynode': 0.49463250898051203,
  'drop_rate': 0.2284900696444142,
  'min_split_gain': 1},
 {'p_min_data_in_leaf': 0.011233294819911024,
  'num_leaves': 4178,
  'learning_rate': 0.024588284895930054,
  'feature_fraction': 0.3052607165804554,
  'feature_fraction_bynode': 0.589252453503204,
  'drop_rate': 0.21634659542789617,
  'min_split_gain': 7}]

Todos los min_split_gain resultan muy bajos, esto no parece bueno, riesgo alto de overfitting

En backtesting contra 202105 el max gain está en torno a 75millones, valor muy bajo, incluso peor que cuando entrenamos con 2 meses

Al evaluar la importancia de las columnas hay al menos 100 columnas con puntaje de 0

Vamos a probar de hacer backtesting dropeando esas columnas sobre los mismos top 3 modelos hallados

Sin embargo corresponderia volver a hacer optimizacion usando un limite inferior de min_split_gain mas alto.


------------

Volvi a correr el entrenamiento sobre esos modelos dropeando las 100 columnas menos importantes. La ganancia de los modelos sigue siendo baja, en torno a los 75millones.

Al hacer test de wilcoxon sobre cada modelo respecto al backtesting en mayo, concluyo que no hay evidencia estadística para afirmar que hay una mejora en los modelos.

-------------

Es evidente que el min_gain_split me jugó una mala pasada, aun sin ser un gran puntaje la ganancia para iteraciones de optimizacion alcanzaron 10.8 millones. La caida de ganancia en backtesting muestra claramente un overfitting.

Probamos con min_gain_split = 7000

------------

