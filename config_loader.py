from datetime import datetime
import pandas as pd
import numpy as np

class ConfigLoader:
    @staticmethod
    def load_config_from_csv(file_path):
        """
        Carga una configuración desde un archivo CSV en formato clave-valor con encabezado.
        :param file_path: Ruta al archivo CSV.
        :return: Diccionario con la configuración.
        """
        try:
            df = pd.read_csv(file_path, index_col=0)

            # Validar existencia y no nulidad de n_series
            if "n_series" not in df.index or pd.isna(df.loc["n_series"].values[0]):
                raise ValueError("Error: El parámetro 'n_series' es obligatorio y no puede estar nulo o no definido.")
            n_series = int(df.loc["n_series"].values[0])

            # Validar nulos, tipo de datos y tamaño de listas
            config = {}
            anomalies = {}  # Nuevo diccionario para almacenar las anomalías
            for key in ["tipo_simulacion", "n_points", "n_series", "ar_params", "ma_params", "means", "stds", "corr_matrix", "start_date", "end_date"]:
                if key not in df.index or pd.isna(df.loc[key].values[0]):
                    raise ValueError(f"Error: El parámetro '{key}' es obligatorio y no puede estar nulo o no definido.")
                try:
                    if key not in ["start_date","end_date"]:
                        value = eval(df.loc[key].values[0])
                        
                        # Validar tipo de datos
                        if key in ["n_points", "n_series"] and not isinstance(value, int):
                            raise ValueError(f"Error: El parámetro '{key}' debe ser un entero, pero se encontró {type(value).__name__}.")
                        
                        if key in ["ar_params", "ma_params", "means", "stds"] and not isinstance(value, list):
                            raise ValueError(f"Error: El parámetro '{key}' debe ser una lista, pero se encontró {type(value).__name__}.")
                            
                        # Validar tamaño de listas
                        if key in ["ar_params", "ma_params", "means", "stds"] and len(value) != n_series:
                            raise ValueError(f"Error: El parámetro '{key}' debe tener {n_series} elementos, pero tiene {len(value)}.")

                        if key == "corr_matrix":
                            # Validar que la matriz sea cuadrada
                            if not all(len(row) == n_series for row in value) or len(value) != n_series:
                                raise ValueError(f"Error: El parámetro '{key}' debe ser una matriz cuadrada de tamaño {n_series}x{n_series}.")

                            # Validar criterios estadísticos de la matriz de correlación
                            value_np = np.array(value)

                            # Validar simetría
                            if not np.allclose(value_np, value_np.T):
                                raise ValueError(f"Error: El parámetro '{key}' debe ser una matriz simétrica.")

                            # Validar diagonal principal
                            if not np.allclose(np.diag(value_np), 1):
                                raise ValueError(f"Error: El parámetro '{key}' debe tener valores de 1 en la diagonal principal.")

                            # Validar rango de valores
                            if not np.all((-1.0 <= value_np) & (value_np <= 1.0)):
                                raise ValueError(f"Error: El parámetro '{key}' contiene valores fuera del rango [-1, 1].")

                            # Validar definida positiva
                            if not np.all(np.linalg.eigvals(value_np) > 0):
                                raise ValueError(f"Error: El parámetro '{key}' debe ser una matriz definida positiva.")
                    if key in ["start_date","end_date"]:
                        value = df.loc[key].values[0]
                    config[key] = value
                except (SyntaxError, NameError) as e:
                    raise ValueError(f"Error: El parámetro '{key}' contiene un valor inválido: {df.loc[key].values[0]}. Detalles: {e}")

            # Convertir valores obligatorios a tipos correctos
            config["n_points"] = int(config["n_points"])
            config["n_series"] = int(config["n_series"])

            # Validar y agregar opcionales
            optional_keys = ["trend_slopes", "seasonality_periods", "seasonality_amplitudes"]
            for key in optional_keys:
                if key in df.index and pd.notna(df.loc[key].values[0]):
                    try:
                        value = eval(df.loc[key].values[0])
                        config[key] = value
                    except (SyntaxError, NameError) as e:
                        raise ValueError(f"Error: El parámetro opcional '{key}' contiene un valor inválido: {df.loc[key].values[0]}. Detalles: {e}")
                else:
                    config[key] = None

            # Detectar y manejar configuraciones de anomalías
            for key in df.index:
                if key.startswith("anomaly_"):
                    try:
                        anomalies[key] = eval(df.loc[key, "value"])  # Evaluar el contenido como diccionario
                    except (SyntaxError, NameError) as e:
                        raise ValueError(f"Error: La configuración de anomalía '{key}' contiene un valor inválido. Detalles: {e}")

            # Agregar las anomalías al diccionario de configuración
            config["anomalies"] = anomalies
            return config
        except Exception as e:
            raise ValueError(f"Error al cargar la configuración desde CSV: {e}")