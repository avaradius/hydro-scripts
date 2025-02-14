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
from threading import Lock

logging.basicConfig(
    filename="error_log.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

simulacion_lock= Lock()

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

def prepare_simulation_data(config_file, timestamp, months_to_add=None):
    config = ConfigLoader.load_config_from_csv(config_file)

    start_date = config.get("start_date", timestamp)

    if months_to_add:
        end_date = TimePeriodHelper.add_months(start_date, months_to_add)
    else:
        end_date = str(config['end_date']) if 'end_date' in config and pd.notna(config['end_date']) else timestamp

    tipo_simulacion = config.get("tipo_simulacion", None)
    total_minutes = TimePeriodHelper.calculate_minutes(start_date, end_date)
    config['n_points'] = total_minutes
    timestamps = TimePeriodHelper.generate_timestamps(start_date, end_date)

    return config, timestamps, tipo_simulacion

def process_simulation(simulator, mode_sim, config):
    if mode_sim == "from_scratch":
        return simulator.simulate(mode=mode_sim, config=config)

    elif mode_sim == "analyze_and_simulate":
        existing_series_file = "../Input/serie_existente.csv"
        if not os.path.exists(existing_series_file):
            raise FileNotFoundError(f"El archivo de series existentes no se encontró: {existing_series_file}")

        existing_series = pd.read_csv(existing_series_file)
        print("Series existentes cargadas correctamente.")
        return simulator.simulate(mode=mode_sim, config=config, time_series=existing_series, period=12, steps=config.get("n_points"))

def save_simulation_results(output_dir, config_file, timestamp, seed, mode_sim, series, id_plc):
    os.makedirs(output_dir, exist_ok=True)

    # Guardar las series en CSV
    filename = f"simulation_results_id_plc_{id_plc}.csv"
    series_csv_path = os.path.join(output_dir, filename)
    series.to_csv(series_csv_path, index=False)

    # Guardar la configuración de la simulación
    save_simulation_config(output_dir, config_file, timestamp, seed, mode_sim)


def load_historico(db, config_file, ids_plc, flags, config_json):
    try:
        session = db.Session()
        db_ops = DatabaseOperations(session)
        simulator = ProcessSimulator()
        ids_metadata = []
        table_name = 'historicos'
        next_id_simulacion = get_next_simulacion_id(session)

        for id_plc in ids_plc:
            seed = int(time.time() * 1000) % 10000
            print(f"Semilla generada: {seed}")

            np.random.seed(seed)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"timestamp de la ejecución: {timestamp}")

            config, timestamps, tipo_simulacion = prepare_simulation_data(config_file, timestamp)

            if tipo_simulacion not in [0, 1]:
                raise ValueError(f"Modo de simulación no válido: {tipo_simulacion}")

            mode_sim = "from_scratch" if tipo_simulacion == 1 else "analyze_and_simulate"
            series = process_simulation(simulator, mode_sim, config)

            new_config = Config(timestamp=timestamp, tipo_simulacion=mode_sim, seed=seed, config=config_json)
            id_metadata = db_ops.insert(new_config)
            if id_metadata:
                ids_metadata.append(id_metadata)

            db_ops.insert_historicos_from_dataframe(session, timestamps, series, id_plc, next_id_simulacion, ids_metadata)
            save_simulation_results("../Output/", config_file, timestamp, seed, mode_sim, series, id_plc)

        db_ops.insert_simulacion(session, next_id_simulacion, ids_metadata, mode_sim, table_name)
        db_ops.clean_temp_simulacion(session, next_id_simulacion)

        flags['load_historico'] = True

    except Exception as e:
        flags['load_historico'] = False
        logging.error(f"Error en load_historico: {e}")
        raise


def add_historico_periodic_record(db, config_file, id_plc, flags, config_json):
    try:
        session = db.Session()
        db_ops = DatabaseOperations(session)
        simulator = ProcessSimulator()
        next_id_simulacion = get_next_simulacion_id(session)
        table_name = 'historicos'
        print(f"start")
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"Iniciando carga periódica a partir de: {timestamp}")

        while True:
            seed = int(time.time() * 1000) % 10000
            print(f"Semilla generada: {seed}")

            np.random.seed(seed)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"timestamp de la ejecución: {timestamp}")

            config, timestamps, tipo_simulacion = prepare_simulation_data(config_file, timestamp, months_to_add=1)

            if tipo_simulacion not in [0, 1]:
                raise ValueError(f"Modo de simulación no válido: {tipo_simulacion}")

            mode_sim = "from_scratch" if tipo_simulacion == 1 else "analyze_and_simulate"
            series = process_simulation(simulator, mode_sim, config)

            new_config = Config(timestamp=timestamp, tipo_simulacion=mode_sim, seed=seed, config=config_json)
            id_metadata = db_ops.insert(new_config)

            db_ops.insert_simulacion(session, next_id_simulacion, [id_metadata], mode_sim, table_name)
            db_ops.clean_temp_simulacion(session, next_id_simulacion)
            save_simulation_results("../Output/", config_file, timestamp, seed, mode_sim, series, id_plc)
            db_ops.insert_historicos_from_dataframe_delay(session, timestamps, series, id_plc, next_id_simulacion)

            flags[f"add_periodic_records_plc_{id_plc}"] = True

    except Exception as e:
        flags[f"add_periodic_records_plc_{id_plc}"] = False
        logging.error(f"Error en el hilo de id_plc {id_plc}: {e}")
        raise


def load_historico_testing(db, config_file, ids_plc, flags, config_json):
    try:
        session = db.Session()
        db_ops = DatabaseOperations(session)
        simulator = ProcessSimulator()
        ids_metadata = []
        table_name = 'historicos_testing'
        next_id_simulacion = get_next_simulacion_id(session)

        for id_plc in ids_plc:
            seed = int(time.time() * 1000) % 10000
            print(f"Semilla generada: {seed}")

            np.random.seed(seed)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"timestamp de la ejecución: {timestamp}")

            config, timestamps, tipo_simulacion = prepare_simulation_data(config_file, timestamp)

            if tipo_simulacion not in [0, 1]:
                raise ValueError(f"Modo de simulación no válido: {tipo_simulacion}")

            mode_sim = "from_scratch" if tipo_simulacion == 1 else "analyze_and_simulate"
            series = process_simulation(simulator, mode_sim, config)

            new_config = Config(timestamp=timestamp, tipo_simulacion=mode_sim, seed=seed, config=config_json)
            id_metadata = db_ops.insert(new_config)
            if id_metadata:
                ids_metadata.append(id_metadata)

            db_ops.insert_historicos_testing_from_dataframe(session, timestamps, series, id_plc, next_id_simulacion, ids_metadata)
            save_simulation_results("../Output/", config_file, timestamp, seed, mode_sim, series, id_plc)

        db_ops.insert_simulacion(session, next_id_simulacion, ids_metadata, mode_sim, table_name)
        db_ops.clean_temp_simulacion(session, next_id_simulacion)

        flags['load_historico'] = True

    except Exception as e:
        flags['load_historico'] = False
        logging.error(f"Error en load_historico: {e}")
        raise


def add_historico_testing_periodic_record(db, config_file, id_plc, flags, config_json):
    try:
        session = db.Session()
        db_ops = DatabaseOperations(session)
        simulator = ProcessSimulator()
        next_id_simulacion = get_next_simulacion_id(session)
        table_name = 'historicos_testing'
        print(f"start")
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"Iniciando carga periódica a partir de: {timestamp}")

        while True:
            seed = int(time.time() * 1000) % 10000
            print(f"Semilla generada: {seed}")

            np.random.seed(seed)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"timestamp de la ejecución: {timestamp}")

            config, timestamps, tipo_simulacion = prepare_simulation_data(config_file, timestamp, months_to_add=1)

            if tipo_simulacion not in [0, 1]:
                raise ValueError(f"Modo de simulación no válido: {tipo_simulacion}")

            mode_sim = "from_scratch" if tipo_simulacion == 1 else "analyze_and_simulate"
            series = process_simulation(simulator, mode_sim, config)

            new_config = Config(timestamp=timestamp, tipo_simulacion=mode_sim, seed=seed, config=config_json)
            id_metadata = db_ops.insert(new_config)

            db_ops.insert_simulacion(session, next_id_simulacion, [id_metadata], mode_sim, table_name)
            db_ops.clean_temp_simulacion(session, next_id_simulacion)
            save_simulation_results("../Output/", config_file, timestamp, seed, mode_sim, series, id_plc)
            db_ops.insert_historicos_testing_from_dataframe_delay(session, timestamps, series, id_plc, next_id_simulacion)

            flags[f"add_periodic_records_plc_{id_plc}"] = True

    except Exception as e:
        flags[f"add_periodic_records_plc_{id_plc}"] = False
        logging.error(f"Error en el hilo de id_plc {id_plc}: {e}")
        raise

def load_monitoreo_vw(db, config_file, ids_plc, flags, config_json):
    try:
        session = db.Session()
        db_ops = DatabaseOperations(session)
        simulator = ProcessSimulator()
        ids_metadata = []
        table_name = 'Monitoreo_vw'
        next_id_simulacion = get_next_simulacion_id(session)

        for id_plc in ids_plc:
            seed = int(time.time() * 1000) % 10000
            print(f"Semilla generada: {seed}")

            np.random.seed(seed)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"timestamp de la ejecución: {timestamp}")

            config, timestamps, tipo_simulacion = prepare_simulation_data(config_file, timestamp)

            if tipo_simulacion not in [0, 1]:
                raise ValueError(f"Modo de simulación no válido: {tipo_simulacion}")

            mode_sim = "from_scratch" if tipo_simulacion == 1 else "analyze_and_simulate"
            series = process_simulation(simulator, mode_sim, config)

            new_config = Config(timestamp=timestamp, tipo_simulacion=mode_sim, seed=seed, config=config_json)
            id_metadata = db_ops.insert(new_config)
            if id_metadata:
                ids_metadata.append(id_metadata)

            db_ops.insert_monitoreo_vw_from_dataframe(session, timestamps, series, id_plc, next_id_simulacion, ids_metadata)
            save_simulation_results("../Output/", config_file, timestamp, seed, mode_sim, series, id_plc)

        db_ops.insert_simulacion(session, next_id_simulacion, ids_metadata, mode_sim, table_name)
        db_ops.clean_temp_simulacion(session, next_id_simulacion)

        flags['load_historico'] = True

    except Exception as e:
        flags['load_historico'] = False
        logging.error(f"Error en load_historico: {e}")
        raise


def add_monitoreo_vw_periodic_record(db, config_file, id_plc, flags, config_json):
    try:
        session = db.Session()
        db_ops = DatabaseOperations(session)
        simulator = ProcessSimulator()
        next_id_simulacion = get_next_simulacion_id(session)
        table_name = 'Monitoreo_vw'
        print(f"start")
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"Iniciando carga periódica a partir de: {timestamp}")

        while True:
            seed = int(time.time() * 1000) % 10000
            print(f"Semilla generada: {seed}")

            np.random.seed(seed)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"timestamp de la ejecución: {timestamp}")

            config, timestamps, tipo_simulacion = prepare_simulation_data(config_file, timestamp, months_to_add=1)

            if tipo_simulacion not in [0, 1]:
                raise ValueError(f"Modo de simulación no válido: {tipo_simulacion}")

            mode_sim = "from_scratch" if tipo_simulacion == 1 else "analyze_and_simulate"
            series = process_simulation(simulator, mode_sim, config)

            new_config = Config(timestamp=timestamp, tipo_simulacion=mode_sim, seed=seed, config=config_json)
            id_metadata = db_ops.insert(new_config)

            db_ops.insert_simulacion(session, next_id_simulacion, [id_metadata], mode_sim, table_name)
            db_ops.clean_temp_simulacion(session, next_id_simulacion)
            save_simulation_results("../Output/", config_file, timestamp, seed, mode_sim, series, id_plc)
            db_ops.insert_monitoreo_vw_from_dataframe_delay(session, timestamps, series, id_plc, next_id_simulacion)

            flags[f"add_periodic_records_plc_{id_plc}"] = True

    except Exception as e:
        flags[f"add_periodic_records_plc_{id_plc}"] = False
        logging.error(f"Error en el hilo de id_plc {id_plc}: {e}")
        raise

def get_next_simulacion_id(session):
    with simulacion_lock:
        try:
            max_id_simulacion = session.query(func.max(Simulacion.id_simulacion)).scalar()
            next_id_simulacion = 1 if max_id_simulacion is None else max_id_simulacion + 1

            placeholder_id_metadata = session.query(Config.id_metadata).filter_by(tipo_simulacion="placeholder").scalar()

            if placeholder_id_metadata is None:
                new_config = Config(
                    timestamp=datetime.now(),
                    tipo_simulacion="placeholder",
                    seed=0,
                    config="{}"
                )
                session.add(new_config)
                session.commit()
                placeholder_id_metadata = new_config.id_metadata
                print(f"Creado id_metadata temporal: {placeholder_id_metadata}")

            new_simulacion = Simulacion(
                id_simulacion=next_id_simulacion, 
                tipo_simulacion=None, 
                id_metadata=placeholder_id_metadata
            )
            session.add(new_simulacion)
            session.commit()

            print(f"Reservado id_simulacion: {next_id_simulacion} con id_metadata {placeholder_id_metadata}")

            return next_id_simulacion

        except SQLAlchemyError as e:
            session.rollback()
            print(f"Error al obtener next_id_simulacion: {e}")
            return None

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

    thread_historico_testing = Thread(target=load_historico_testing, args=(db, config_file, ids_plc, flags, config_json))
    thread_historico_testing.start()
    threads.append(thread_historico_testing)

    thread_monitoreo_vw = Thread(target=load_monitoreo_vw, args=(db, config_file, ids_plc, flags, config_json))
    thread_monitoreo_vw.start()
    threads.append(thread_monitoreo_vw)
 
    for id_plc in ids_plc:
        thread = Thread(target=add_historico_periodic_record, args=(db, config_file, id_plc, flags, config_json))
        thread.start()
        threads.append(thread)

    for id_plc in ids_plc:
        thread = Thread(target=add_historico_testing_periodic_record, args=(db, config_file, id_plc, flags, config_json))
        thread.start()
        threads.append(thread)
    
    for id_plc in ids_plc:
        thread = Thread(target=add_monitoreo_vw_periodic_record, args=(db, config_file, id_plc, flags, config_json))
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