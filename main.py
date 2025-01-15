import os
import time
import numpy as np
import pandas as pd
import json
from datetime import datetime
from process_simulator import ProcessSimulator
from config_loader import ConfigLoader
from series_visualizer import SeriesVisualizer

def save_simulation_config(output_dir, config_file, timestamp, seed, mode_sim):
    """
    Guarda la configuración de la simulación en un archivo CSV.
    """
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

    # Generar una semilla basada en la marca de tiempo
    seed = int(time.time() * 1000) % 10000  # Últimos 4 dígitos del tiempo actual
    print(f"Semilla generada: {seed}")

    # Establecer la semilla global
    np.random.seed(seed)

    # Obtener el timestamp actual
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Timestamp de la ejecución: {timestamp}")

    config_file = "../Input/config.csv"
    config = ConfigLoader.load_config_from_csv(config_file)

    simulator = ProcessSimulator()

    # Verificar el valor de tipo_simulacion
    tipo_simulacion = config.get("tipo_simulacion", None)

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

    # Crear carpeta Output si no existe
    output_dir = os.path.join("../Output/")
    os.makedirs(output_dir, exist_ok=True)

    # Guardar las series
    series_csv_path = os.path.join(output_dir, "simulation_results.csv")
    series.to_csv(series_csv_path, index=False)
    print(f"Series generadas en: {series_csv_path}")

    # Guardar la configuración de la simulación
    save_simulation_config(output_dir, config_file, timestamp, seed, mode_sim)

    # Usar la clase SeriesVisualizer para graficar
    # SeriesVisualizer.plot_series(series, title="Ejemplo: Series Generadas")
    SeriesVisualizer.plot_individual_series(series, base_title="Ejemplo: Serie")

if __name__ == "__main__":
    main()