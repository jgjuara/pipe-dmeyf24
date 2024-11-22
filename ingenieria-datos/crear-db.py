

def procesamiento_base(dbname, file):

    import duckdb
    import pandas as pd
    import numpy as np
    from pathlib import Path
    import re
    import logging
    from datetime import datetime

    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


     # Log init time of the process
    init_time = datetime.now()
    logging.info(f"Process started at {init_time}")


    root_path = Path(__file__).parent.parent.resolve()

    root_path = root_path.as_posix() + '/'

    # Connect to DuckDB database
    con = duckdb.connect(root_path + 'duckdb/' + dbname + ".duckdb")

    # Load data from CSV
    csv_path = root_path + "datasets/" + file
    
    df = pd.read_csv(csv_path, compression='gzip')

    # Create a table in DuckDB and insert the data
    con.execute(f"CREATE OR REPLACE TABLE {dbname} AS SELECT * FROM df")

    del df

    con.execute("""CREATE OR REPLACE MACRO suma_sin_null(a, b) AS ifnull(a, 0) + ifnull(b, 0);""")

    con.execute(
    """CREATE OR REPLACE MACRO division_segura(a, b) AS 
        CASE 
            WHEN ifnull(b, 0) = 0 THEN NULL 
            ELSE ifnull(a, 0) / b 
        END;""" 
    )

    #%% creacion target

    query = f"""create or replace table {dbname} as
    with periodos as (
    select distinct foto_mes from {dbname}
    ), clientes as (
    select distinct numero_de_cliente from {dbname}
    ), todo as (
    select numero_de_cliente, foto_mes from clientes cross join periodos
    ), clase_ternaria as (
    select
    c.*
        , if(c.numero_de_cliente is null, 0, 1) as mes_0
    , lead(mes_0, 1) over (partition by t.numero_de_cliente order by foto_mes) as mes_1
    , lead(mes_0, 2) over (partition by t.numero_de_cliente order by foto_mes) as mes_2
    , if (mes_2 = 1, 'CONTINUA',
            if (mes_1 = 1 and mes_2 = 0, 'BAJA+2',
                if (mes_1 = 0 and mes_2 = 0, 'BAJA+1', null))) as clase_ternaria
    from todo t
    left join {dbname} c using (numero_de_cliente, foto_mes)
    ) select
    * EXCLUDE (mes_0, mes_1, mes_2)
    from clase_ternaria
    where mes_0 = 1"""


    con.execute(query)

    #%% fix ctrx_quarter

    # columnas = con.execute(f"""SELECT column_name
    # FROM information_schema.columns
    # WHERE table_name = '{dbname}';""").fetchdf()

    # columnas = columnas["column_name"]

    # columnas = columnas[~columnas.str.contains("ctrx_quarter")]

    # columnas = ", ".join(columnas)

    # # print(columnas)

    # query = f"""
    # create or replace table {dbname} as
    #     select {columnas},
    #     CASE WHEN cliente_antiguedad = 1 THEN ctrx_quarter * 3
    #         WHEN cliente_antiguedad = 2 THEN ctrx_quarter * 3 / 2
    #         ELSE ctrx_quarter
    #     END AS ctrx_quarter,
    # from {dbname};
    # """

    # print(query)

    # con.execute(query)

    # #%% creacion de columnas adicionales



    # # limite credito compra total ----------------------------------------------------

    # con.execute(f"""
    # create or replace table {dbname} as
    #     select *,
    #     suma_sin_null(Master_mlimitecompra, Visa_mlimitecompra) as mtotal_limitecompra
    #     from {dbname};
    # """)

    # # limite financiacion total -----------------------------------------------

    # # Visa_mfinanciacion_limite + Master_mfinanciacion_limite

    # query =  "ifnull(Visa_mfinanciacion_limite,0) + ifnull(Master_mfinanciacion_limite, 0) as mtotal_financiacion_limite"

    # con.execute(f"""
    # create or replace table {dbname} as
    #     select *, 
    #     {query}
    #     from {dbname};
    # """)
                

    # #%%

    # # Master_msaldototal + Visa_msaldototal

    # query = "ifnull(Master_msaldototal, 0) + ifnull(Visa_msaldototal, 0) as mtotal_saldototal"

    # con.execute(f"""
    # create or replace table {dbname} as
    #     select *, 
    #     {query}
    #     from {dbname};
    # """)


    # #  patrimonio en cuenta ---------------------------------------------------

    # # mcuentas_saldo + minversion1_pesos + minversion2 + mplazo_fijo_dolares +mplazo_fijo_pesos


    # query = "ifnull(mcuentas_saldo, 0) + ifnull(minversion1_pesos, 0) + ifnull(minversion2, 0) + ifnull(mplazo_fijo_dolares, 0) + ifnull(mplazo_fijo_pesos, 0) as mtotal_patrimonio"

    # con.execute(f"""
    # create or replace table {dbname} as
    #     select *, 
    #     {query}
    #     from {dbname};
    # """)


    # # prestamos  ---------------------------------------------------

    # # mprestamos_personales + mprestamos_prendarios + mprestamos_hipotecarios

    # query = "ifnull(mprestamos_personales, 0) + ifnull(mprestamos_prendarios, 0) + ifnull(mprestamos_hipotecarios, 0) as mtotal_prestamos"

    # con.execute(f"""
    # create or replace table {dbname} as
    #     select *, 
    #     {query}
    #     from {dbname};
    # """)

    # #%%


    # columnas = con.execute(f"""SELECT column_name
    # FROM information_schema.columns
    # WHERE table_name = '{dbname}';""").fetchdf()

    # columnas = columnas["column_name"]

    # print(columnas.to_list())

    # con.execute(f"""
    # create or replace table {dbname} as
    # select *,
    #     suma_sin_null(mtarjeta_visa_consumo, mtarjeta_master_consumo) as mtc_consumo_total
    #     , suma_sin_null(mttarjeta_visa_debitos_automaticos, mttarjeta_master_debitos_automaticos) as mtc_debitosautomaticos_total
    #     , suma_sin_null(Master_mfinanciacion_limite,Visa_mfinanciacion_limite) as mtc_financiacionlimite_total
    #     , suma_sin_null(Master_msaldopesos,Visa_msaldopesos) as mtc_saldopesos_total
    #     , suma_sin_null(Master_msaldodolares,Visa_msaldodolares) as mtc_saldodolares_total
    #     , suma_sin_null(Master_mconsumospesos,Visa_mconsumospesos) as mtc_consumopesos_total
    #     , suma_sin_null(Master_mconsumosdolares,Visa_mconsumosdolares) as mtc_consumodolares_total
    #     , suma_sin_null(Master_mlimitecompra,Visa_mlimitecompra) as mtc_limitecompra_total
    #     , suma_sin_null(Master_madelantopesos,Visa_madelantopesos) as mtc_adelantopesos_total
    #     , suma_sin_null(Master_madelantodolares,Visa_madelantodolares) as mtc_adelantodolares_total
    #     , suma_sin_null(mtc_adelantopesos_total,mtc_adelantodolares_total) as mtc_adelanto_total
    #     , suma_sin_null(Master_mpagado,Visa_mpagado) as mtc_pagado_total
    #     , suma_sin_null(Master_mpagospesos,Visa_mpagospesos) as mtc_pagadopesos_total
    #     , suma_sin_null(Master_mpagosdolares,Visa_mpagosdolares) as mtc_pagadodolares_total
    #     , suma_sin_null(Master_msaldototal,Visa_msaldototal) as mtc_saldototal_total
    #     , suma_sin_null(Master_mconsumototal,Visa_mconsumototal) as mtc_consumototal_total
    #     , suma_sin_null(Master_cconsumos,Visa_cconsumos) as tc_cconsumos_total
    #     , suma_sin_null(Master_delinquency,Visa_delinquency) as tc_morosidad_total
    # from {dbname};
    # """)

    # #%%
    # con.execute(f"""
    #             create or replace table {dbname} as
    # select *
    #     , suma_sin_null(mplazo_fijo_dolares, mplazo_fijo_pesos) as m_plazofijo_total
    #     , suma_sin_null(minversion1_dolares, minversion1_pesos) as m_inversion1_total
    #     , suma_sin_null(mpayroll, mpayroll2) as m_payroll_total
    #     , suma_sin_null(cpayroll_trx, cpayroll2_trx) as c_payroll_total
    #     , suma_sin_null(suma_sin_null(suma_sin_null(cseguro_vida, cseguro_auto), cseguro_vivienda), cseguro_accidentes_personales) as c_seguros_total
    # from {dbname};
    # """)

    # #%%
    # con.execute(f"""
    # create or replace table {dbname} as
    # select
    #     *
    #     , greatest(Master_Fvencimiento, Visa_Fvencimiento) as tc_fvencimiento_mayor
    #     , least(Master_Fvencimiento, Visa_Fvencimiento) as tc_fvencimiento_menor
    #     , greatest(Master_fechaalta, Visa_fechaalta) as tc_fechaalta_mayor
    #     , least(Master_fechaalta, Visa_fechaalta) as tc_fechalta_menor
    #     , greatest(Master_Finiciomora,Visa_Finiciomora) as tc_fechamora_mayor
    #     , least(Master_Finiciomora,Visa_Finiciomora) as tc_fechamora_menor
    #     , greatest(Master_fultimo_cierre,Visa_fultimo_cierre) as tc_fechacierre_mayor
    #     , least(Master_fultimo_cierre,Visa_fultimo_cierre) as tc_fechacierre_menor
    # from {dbname};
    # """)

    # #%%
    # con.execute(f"""
    # create or replace table {dbname} as
    # select
    #     *
    #     , ntile(10) over (partition by foto_mes order by cliente_antiguedad) as cliente_antiguedad_decil 
    # from {dbname}
    # order by numero_de_cliente, cliente_antiguedad
    # """)

    # con.execute(f"""
    # create or replace table {dbname} as
    # select
    #     *
    #     ,ntile(10) over (partition by foto_mes order by tc_fechaalta_mayor) as antiguedad_tarjetas
    # from {dbname}
    # order by numero_de_cliente, tc_fechaalta_mayor
    # """)

    # #%%
    # con.execute(f"""
    # create or replace table {dbname} as
    # select *
    #     , division_segura(m_plazofijo_total, cplazo_fijo) as m_promedio_plazofijo_total
    #     , division_segura(m_inversion1_total, cinversion1) as m_promedio_inversion_total
    #     , division_segura(mcaja_ahorro, ccaja_ahorro) as m_promedio_caja_ahorro
    #     , division_segura(mtarjeta_visa_consumo, ctarjeta_visa_transacciones) as m_promedio_tarjeta_visa_consumo_por_transaccion
    #     , division_segura(mtarjeta_master_consumo, ctarjeta_master_transacciones) as m_promedio_tarjeta_master_consumo_por_transaccion
    #     , division_segura(mprestamos_personales, cprestamos_personales) as m_promedio_prestamos_personales
    #     , division_segura(mprestamos_prendarios, cprestamos_prendarios) as m_promedio_prestamos_prendarios
    #     , division_segura(mprestamos_hipotecarios, cprestamos_hipotecarios) as m_promedio_prestamos_hipotecarios
    #     , division_segura(minversion2, cinversion2) as m_promedio_inversion2
    #     , division_segura(mpagodeservicios, cpagodeservicios) as m_promedio_pagodeservicios
    #     , division_segura(mpagomiscuentas, cpagomiscuentas) as m_promedio_pagomiscuentas
    #     , division_segura(mcajeros_propios_descuentos, ccajeros_propios_descuentos) as m_promedio_cajeros_propios_descuentos
    #     , division_segura(mtarjeta_visa_descuentos, ctarjeta_visa_descuentos) as m_promedio_tarjeta_visa_descuentos
    #     , division_segura(mtarjeta_master_descuentos, ctarjeta_master_descuentos) as m_promedio_tarjeta_master_descuentos
    #     , division_segura(mcomisiones_mantenimiento, ccomisiones_mantenimiento) as m_promedio_comisiones_mantenimiento
    #     , division_segura(mcomisiones_otras, ccomisiones_otras) as m_promedio_comisiones_otras
    #     , division_segura(mforex_buy, cforex_buy) as m_promedio_forex_buy
    #     , division_segura(mforex_sell, cforex_sell) as m_promedio_forex_sell
    #     , division_segura(mtransferencias_recibidas, ctransferencias_recibidas) as m_promedio_transferencias_recibidas
    #     , division_segura(mtransferencias_emitidas, ctransferencias_emitidas) as m_promedio_transferencias_emitidas
    #     , division_segura(mextraccion_autoservicio, cextraccion_autoservicio) as m_promedio_extraccion_autoservicio
    #     , division_segura(mcheques_depositados, ccheques_depositados) as m_promedio_cheques_depositados
    #     , division_segura(mcheques_emitidos, ccheques_emitidos) as m_promedio_cheques_emitidos
    #     , division_segura(mcheques_depositados_rechazados, ccheques_depositados_rechazados) as m_promedio_cheques_depositados_rechazados
    #     , division_segura(mcheques_emitidos_rechazados, ccheques_emitidos_rechazados) as m_promedio_cheques_emitidos_rechazados
    #     , division_segura(matm, catm_trx) as m_promedio_atm
    #     , division_segura(matm_other, catm_trx_other) as m_promedio_atm_other
    #     , division_segura(Master_msaldototal,Master_mfinanciacion_limite) as proporcion_financiacion_master_cubierto
    #     , division_segura(Master_msaldototal,Master_mlimitecompra) as proporcion_limite_master_cubierto
    #     , division_segura(Visa_msaldototal,Visa_mfinanciacion_limite) as proporcion_financiacion_visa_cubierto
    #     , division_segura(Visa_msaldototal,Visa_mlimitecompra) as proporcion_limite_visa_cubierto
    #     , division_segura(mtc_saldototal_total,mtc_financiacionlimite_total) as proporcion_financiacion_total_cubierto
    #     , division_segura(mtc_saldototal_total,mtc_limitecompra_total) as proporcion_limite_total_cubierto
    #     , division_segura(mtc_saldopesos_total,mtc_saldototal_total) as tc_proporcion_saldo_pesos
    #     , division_segura(mtc_saldodolares_total,mtc_saldototal_total) as tc_proporcion_saldo_dolares
    #     , division_segura(mtc_consumopesos_total,mtc_consumototal_total) as tc_proporcion_consumo_pesos
    #     , division_segura(mtc_consumodolares_total,mtc_consumototal_total) as tc_proporcion_consumo_dolares
    #     , division_segura(mtc_consumototal_total,mtc_limitecompra_total) as tc_proporcion_consumo_total_limite_total_cubierto
    #     , division_segura(mtc_pagadopesos_total,mtc_pagado_total) as tc_proporcion_pago_pesos
    #     , division_segura(mtc_pagadodolares_total,mtc_pagado_total) as tc_proporcion_pago_dolares
    #     , division_segura(mtc_adelantopesos_total,mtc_adelanto_total) as tc_proporcion_adelanto_pesos
    #     , division_segura(mtc_adelantodolares_total,mtc_adelanto_total) as tc_proporcion_adelanto_dolares
    # from {dbname}
    # """)



    # #%% creacion de lags


    # columnas = con.execute(f"""SELECT column_name
    # FROM information_schema.columns
    # WHERE table_name = '{dbname}';""").fetchdf()

    # columnas = columnas["column_name"]

    # columnas = columnas[~columnas.str.contains("numero_de_cliente|foto_mes|clase_ternaria")]

    # query = ", ".join([f"""LAG({col}, 1) over (partition by numero_de_cliente order by foto_mes) AS lag1_{col},
    #                 LAG({col}, 2) over (partition by numero_de_cliente order by foto_mes) AS lag2_{col}"""
    #                     for col in columnas])

    # con.execute(f"""
    #     CREATE OR REPLACE TABLE {dbname}_lags AS
    #             SELECT numero_de_cliente, foto_mes, clase_ternaria,
    #             {query}
    #             FROM {dbname};""")

    # #%% calcular deltas -query

    # # con.execute("""select numero_de_cliente, foto_mes,
    # #               lag(foto_mes, 1) over (partition by numero_de_cliente order by foto_mes) as lag1_foto_mes,
    # #              lag(foto_mes, 2) over (partition by numero_de_cliente order by foto_mes) as lag2_foto_mes,
    # #             lag1_foto_mes,
    # #             lag2_foto_mes,
    # #             foto_mes/ lag1_foto_mes as delta1_foto_mes,
    # #             foto_mes/ lag2_foto_mes as delta2_foto_mes,
    # #             foto_mes/ - lag1_foto_mes as delta1_foto_mes,
    # #             - foto_mes/ lag2_foto_mes as delta2_foto_mes,
    # #              from competencia_01 limit 50""").fetchdf()



    # columnas = con.execute("""SELECT column_name
    # FROM information_schema.columns
    # WHERE table_name = '{dbname}';""").fetchdf()

    # columnas = columnas["column_name"]

    # columnas = columnas[~columnas.str.contains("numero_de_cliente|foto_mes|clase_ternaria")]

    # # exclude columns that start with 'lag1|lag2'
    # # Use regex to exclude columns that start with 'lag1' or 'lag2'
    # pattern = re.compile(r'^(lag1|lag2)')
    # columnas = [col for col in columnas if not pattern.match(col)]

    # print(columnas)


    # query = ", ".join([f"""({col}/lag1_{col})-1 as variacion1_{col},
    #                 ({col}/lag2_{col})-1 as variacion2_{col}"""
    #                     for col in columnas])

    # con.execute(f"""
    #     CREATE OR REPLACE TABLE {dbname}_deltas AS
    #             SELECT {dbname}.numero_de_cliente, {dbname}.foto_mes, {dbname}.clase_ternaria,
    #             {query}
    #             FROM {dbname}
    #             JOIN {dbname}_lags
    #             ON {dbname}.foto_mes = {dbname}_lags.foto_mes
    #             AND {dbname}.numero_de_cliente = {dbname}_lags.numero_de_cliente
    #             AND {dbname}.clase_ternaria = {dbname}_lags.clase_ternaria;""")


    # #%% columnas montos a decilado

    # columnas = con.execute("""SELECT column_name
    # FROM information_schema.columns
    # WHERE table_name = '{dbname}';""").fetchdf()

    # columnas = columnas["column_name"]

    # pattern = re.compile(r'^m|^lag1_m|^lag2_m')
    # columnas_seleccionadas = [col for col in columnas if pattern.match(col)]

    # # get columns that start with 'm'
    # columnas_noseleccionadas =  [col for col in columnas if col not in columnas_seleccionadas]

    # print(columnas_seleccionadas)


    # query = ", ".join([f"""ntile(200) over (partition by foto_mes order by {col}) as ntile_{col}"""
    #                     for col in columnas_seleccionadas])

    # print(query)

    # con.execute(f"""
    #     CREATE OR REPLACE TABLE competencia_01 AS
    #             SELECT {columnas_noseleccionadas},
    #             {query}
    #             FROM {dbname};""")

    # #%% decilado lags

    # columnas = con.execute(f"""SELECT column_name
    # FROM information_schema.columns
    # WHERE table_name = '{dbname}';""").fetchdf()

    # columnas = columnas["column_name"]

    # columnas = columnas[~columnas.str.contains("numero_de_cliente|foto_mes|clase_ternaria")]

    # query = ", ".join([f"""LAG({col}, 1) over (partition by numero_de_cliente order by foto_mes) AS lag1_{col},
    #                 LAG({col}, 2) over (partition by numero_de_cliente order by foto_mes) AS lag2_{col}"""
    #                     for col in columnas])

    # con.execute(f"""
    #     CREATE OR REPLACE TABLE {dbname}_lags AS
    #             SELECT numero_de_cliente, foto_mes, clase_ternaria,
    #             {query}
    #             FROM {dbname};""")



    con.close()

    # Log final time of the process
    final_time = datetime.now()
    logging.info(f"Process ended at {final_time}")
    logging.info(f"Total duration: {final_time - init_time}")


procesamiento_base(dbname= "compe_02", file = "competencia_02_crudo.csv.gz")