import os
import time
import numpy as np
import pandas as pd
import json
from datetime import datetime
from process_simulator import ProcessSimulator
from config_loader import ConfigLoader
from series_visualizer import SeriesVisualizer
from db_conexion import DatabaseConnection
from models import Config,Simulacion
from crud_operations import DatabaseOperations
from time_period_helper import TimePeriodHelper
from sqlalchemy import func

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

def main():

    db = DatabaseConnection()
    session = db.Session()
    db_ops = DatabaseOperations(session)
    config_file = "../Input/config.csv"
    df_config = pd.read_csv(config_file)
    config_dict = df_config.set_index('parameter')['value'].to_dict()
    config_json = json.dumps(config_dict)
    ids_plc = db_ops.get_ids_plc(session)
    ids_metadata = []
    mode_sim=""
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

        simulator = ProcessSimulator()

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
        
        db_ops.insert_historicos_from_dataframe(session, timestamps, series, id_plc, id_metadata)

        output_dir = os.path.join("../Output/")
        os.makedirs(output_dir, exist_ok=True)

        # Guardar las series
        filename = f"simulation_results_id_plc_{id_plc}.csv"
        series_csv_path = os.path.join(output_dir, filename)
        series.to_csv(series_csv_path, index=False)

        # Guardar la configuración de la simulación
        save_simulation_config(output_dir, config_file, timestamp, seed, mode_sim)

        #SeriesVisualizer.plot_individual_series(series, base_title="Ejemplo: Serie")

    max_id_simulacion = session.query(func.max(Simulacion.id_simulacion)).scalar()
    next_id_simulacion = 1 if max_id_simulacion is None else max_id_simulacion + 1
    db_ops.insert_simulacion(session, next_id_simulacion, ids_metadata, mode_sim)

if __name__ == "__main__":
    main()