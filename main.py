import os
import time
import numpy as np
import pandas as pd
import json
import logging
from datetime import datetime
from process_simulator import ProcessSimulator
from config_loader import ConfigLoader
from series_visualizer import SeriesVisualizer
from db_conexion import DatabaseConnection
from models import Config,Simulacion
from crud_operations import DatabaseOperations
from time_period_helper import TimePeriodHelper
from sqlalchemy import func
from threading import Thread
from typing import List
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(
    filename="error_log.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def save_simulation_config(output_dir, config_file, timestamp, seed, mode_sim):
    df_config = pd.read_csv(config_file)
    config_dict = df_config.set_index('parameter')['value'].to_dict()
    config_json = json.dumps(config_dict)
    
    data = {
        "Tipo Simulacion": mode_sim,
        "Timestamp": [timestamp],
        "Seed": [seed],
        "Config": [config_json]
    }
    df_config = pd.DataFrame(data)

    output_csv_path = os.path.join(output_dir, "simulation_config.csv")
    if os.path.exists(output_csv_path):
        existing_data = pd.read_csv(output_csv_path)
        updated_data = pd.concat([existing_data, df_config], ignore_index=True)
    else:
        updated_data = df_config

    updated_data.to_csv(output_csv_path, index=False)
    print(f"Configuración guardada en: {output_csv_path}")

def load_historico(db = DatabaseConnection, config_file = str, ids_plc: List[int] = None, flags: any = None, config_json: json = None) -> bool:
    try: 
        session = db.Session()
        db_ops = DatabaseOperations(session)
        simulator = ProcessSimulator()
        ids_metadata = []
        max_id_simulacion = session.query(func.max(Simulacion.id_simulacion)).scalar()
        next_id_simulacion = 1 if max_id_simulacion is None else max_id_simulacion + 1
        for id_plc in ids_plc:
            seed = int(time.time() * 1000) % 10000  # Últimos 4 dígitos del tiempo actual
            print(f"Semilla generada: {seed}")

            np.random.seed(seed)
            
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"timestamp de la ejecución: {timestamp}")
            config = ConfigLoader.load_config_from_csv(config_file)
            
            start_date  = config['start_date']
            end_date = str(config['end_date']) if 'end_date' in config and pd.notna(config['end_date']) else None
            if end_date is None:
                end_date = timestamp
            
            tipo_simulacion = config.get("tipo_simulacion", None)

            total_minutes = TimePeriodHelper.calculate_minutes(start_date, end_date)
            config['n_points']=total_minutes
            timestamps = TimePeriodHelper.generate_timestamps(start_date, end_date)

            if tipo_simulacion == 1:
                mode_sim = "from_scratch"
            elif tipo_simulacion == 0:
                mode_sim = "analyze_and_simulate"
            if tipo_simulacion not in [0, 1]:
                raise ValueError(f"Modo de simulación no válido en el archivo de configuración. Debe ser 1 ('from_scratch') o 0 ('analyze_and_simulate'). Valor encontrado: {tipo_simulacion}.")
            
            if mode_sim == "from_scratch":
                # Simulación desde cero
                series = simulator.simulate(mode=mode_sim, config=config)
            elif mode_sim == "analyze_and_simulate":
                # Cargar series de tiempo existentes
                existing_series_file = "../Input/serie_existente.csv"  # Asegúrate de tener este archivo
                if not os.path.exists(existing_series_file):
                    raise FileNotFoundError(f"El archivo de series existentes no se encontró: {existing_series_file}")
                
                existing_series = pd.read_csv(existing_series_file)
                print("Series existentes cargadas correctamente.")
                series = simulator.simulate(
                    mode=mode_sim,
                    config=config,
                    time_series=existing_series,
                    period=12,
                    steps=config.get("n_points")  # Usa `n_points` como número de pasos a simular
                )

            new_config = Config(
            timestamp=timestamp,
            tipo_simulacion=mode_sim,
            seed=seed,
            config=config_json
            )

            id_metadata = db_ops.insert(new_config)
            if id_metadata:
                ids_metadata.append(id_metadata)
            
            db_ops.insert_historicos_from_dataframe(session, timestamps, series, id_plc, next_id_simulacion)

            output_dir = os.path.join("../Output/")
            os.makedirs(output_dir, exist_ok=True)

            # Guardar las series
            filename = f"simulation_results_id_plc_{id_plc}.csv"
            series_csv_path = os.path.join(output_dir, filename)
            series.to_csv(series_csv_path, index=False)

            # Guardar la configuración de la simulación
            save_simulation_config(output_dir, config_file, timestamp, seed, mode_sim)

            #SeriesVisualizer.plot_individual_series(series, base_title="Ejemplo: Serie")

        db_ops.insert_simulacion(session, next_id_simulacion, ids_metadata, mode_sim)
        flags['load_historico'] = True  # Marcar éxito
    
    except Exception as e:
        flags['load_historico'] = False
        logging.error(f"Error en load_historico: {e}")
        raise 

def add_periodic_record(db = DatabaseConnection, config_file = str, id_plc = int, flags: any = None, config_json: json = None):
    try:

        session = db.Session()
        db_ops = DatabaseOperations(session)
        print(f"start")
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"Iniciando carga periódica a partir de: {timestamp}")
        simulator = ProcessSimulator()
        config = ConfigLoader.load_config_from_csv(config_file)
        max_id_simulacion = session.query(func.max(Simulacion.id_simulacion)).scalar()
        next_id_simulacion = 1 if max_id_simulacion is None else max_id_simulacion + 1
        
        while True:
            seed = int(time.time() * 1000) % 10000  # Últimos 4 dígitos del tiempo actual
            print(f"Semilla generada: {seed}")

            np.random.seed(seed)
            
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"timestamp de la ejecución: {timestamp}")
            config = ConfigLoader.load_config_from_csv(config_file)
            
            start_date  = timestamp
            end_date = TimePeriodHelper.add_one_month(start_date)           
            tipo_simulacion = config.get("tipo_simulacion", None)

            total_minutes = TimePeriodHelper.calculate_minutes(start_date, end_date)
            config['n_points']=total_minutes
            timestamps = TimePeriodHelper.generate_timestamps(start_date, end_date)

            if tipo_simulacion == 1:
                mode_sim = "from_scratch"
            elif tipo_simulacion == 0:
                mode_sim = "analyze_and_simulate"
            if tipo_simulacion not in [0, 1]:
                raise ValueError(f"Modo de simulación no válido en el archivo de configuración. Debe ser 1 ('from_scratch') o 0 ('analyze_and_simulate'). Valor encontrado: {tipo_simulacion}.")
            
            if mode_sim == "from_scratch":
                # Simulación desde cero
                series = simulator.simulate(mode=mode_sim, config=config)
            elif mode_sim == "analyze_and_simulate":
                # Cargar series de tiempo existentes
                existing_series_file = "../Input/serie_existente.csv"  # Asegúrate de tener este archivo
                if not os.path.exists(existing_series_file):
                    raise FileNotFoundError(f"El archivo de series existentes no se encontró: {existing_series_file}")
                
                existing_series = pd.read_csv(existing_series_file)
                print("Series existentes cargadas correctamente.")
                series = simulator.simulate(
                    mode=mode_sim,
                    config=config,
                    time_series=existing_series,
                    period=12,
                    steps=config.get("n_points")  # Usa `n_points` como número de pasos a simular
                )

            new_config = Config(
            timestamp=timestamp,
            tipo_simulacion=mode_sim,
            seed=seed,
            config=config_json
            )

            id_metadata = db_ops.insert(new_config)
            db_ops.insert_simulacion(session, next_id_simulacion, [id_metadata], mode_sim)
            db_ops.insert_historicos_from_dataframe_delay(session, timestamps, series, id_plc, next_id_simulacion)

            output_dir = os.path.join("../Output/")
            os.makedirs(output_dir, exist_ok=True)

            # Guardar la configuración de la simulación
            save_simulation_config(output_dir, config_file, timestamp, seed, mode_sim)
            #SeriesVisualizer.plot_individual_series(series, base_title="Ejemplo: Serie")
            flags[f"add_periodic_records_plc_{id_plc}"] = True

    except Exception as e:
        flags[f"add_periodic_records_plc_{id_plc}"] = False
        logging.error(f"Error en el hilo de id_plc {id_plc}: {e}")
        raise

def main():

    db = DatabaseConnection()
    session = db.Session()
    db_ops = DatabaseOperations(session)
    config_file = "../Input/config.csv"
    df_config = pd.read_csv(config_file)
    config_dict = df_config.set_index('parameter')['value'].to_dict()
    config_json = json.dumps(config_dict)
    ids_plc = db_ops.get_ids_plc(session)
    flags = {
        'load_historico': False,
        **{f"add_periodic_records_plc_{id_plc}": False for id_plc in ids_plc}
    }

    threads = []
    thread_historico = Thread(target=load_historico, args=(db, config_file, ids_plc, flags, config_json))
    thread_historico.start()
    threads.append(thread_historico)
 
    for id_plc in ids_plc:
        thread = Thread(target=add_periodic_record, args=(db, config_file, id_plc, flags, config_json))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    if flags['load_historico']:
        print("Carga del histórico completada con éxito.")
    else:
        print("Error durante la carga del histórico. Ver log para más detalles.")

    for id_plc in ids_plc:
        if flags[f"add_periodic_records_plc_{id_plc}"]:
            print(f"Carga periódica para id_plc {id_plc} completada con éxito.")
        else:
            print(f"Error en la carga periódica para id_plc {id_plc}.")

    if not flags['add_periodic_records_plc_1']:
        print("Error en la carga periódica. Ver log para más detalles.")

if __name__ == "__main__":
    main()