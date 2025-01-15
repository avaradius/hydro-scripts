import numpy as np

class AnomalyInjector:
    def __init__(self, anomalies_config):
        """
        Inicializa el inyector con la configuración de anomalías.
        :param anomalies_config: Diccionario con las configuraciones de anomalías.
        """
        self.anomalies_config = anomalies_config

    def inject_anomalies(self, series):
        """
        Aplica las anomalías definidas en la configuración a las series.
        :param series: DataFrame con las series de tiempo a modificar.
        :return: DataFrame con las anomalías inyectadas.
        """
        # Iterar sobre las anomalías definidas en la configuración
        for anomaly_type, params in self.anomalies_config.items():
            if anomaly_type == "anomaly_outliers":
                self._inject_outliers(series, params)
            elif anomaly_type == "anomaly_drift":
                self._inject_drift(series, params)
            elif anomaly_type == "anomaly_std_change":
                self._inject_std_change(series, params)
        return series

    def _inject_outliers(self, series, params):
        """
        Inyecta outliers en las series basándose en múltiplos de la desviación estándar.
        Los outliers pueden ser positivos o negativos de manera aleatoria.
        :param series: DataFrame con las series de tiempo.
        :param params: Diccionario con parámetros para la anomalía.
        """
        series_ids = params.get("series", [])
        n_std = params.get("magnitude", 1)  # Magnitud como múltiplo de la desviación estándar
        count = params.get("count", 1)

        # Asegúrate de que exista la columna "Anomaly"
        if "Anomaly" not in series.columns:
            series["Anomaly"] = False

        for series_id in series_ids:
            # Calcular la desviación estándar de la serie
            std = np.std(series.iloc[:, series_id])
            
            # Seleccionar índices aleatorios
            indices = np.random.choice(len(series), count, replace=False)

            # Modificar las observaciones seleccionadas
            for idx in indices:
                direction = np.random.choice([-1, 1])  # Decidir aleatoriamente si el outlier es positivo o negativo
                series.iloc[idx, series_id] += direction * n_std * std
            
            # Marcar las filas afectadas como anomalías
            series.loc[indices, "Anomaly"] = True

    def _inject_drift(self, series, params):
        """
        Inyecta drift en las series.
        :param series: DataFrame con las series de tiempo.
        :param params: Diccionario con parámetros para la anomalía.
        """
        series_ids = params.get("series", [])
        slope = params.get("slope", 0)
        start_point = params.get("start_point", 0)

        # Asegúrate de que exista la columna "Anomaly"
        if "Anomaly" not in series.columns:
            series["Anomaly"] = False

        for series_id in series_ids:
            drift = np.zeros(len(series))
            drift[start_point:] = slope * np.arange(len(series) - start_point)
            series.iloc[:, series_id] += drift
            
            # Marcar las filas afectadas como anomalías
            series.loc[start_point:, "Anomaly"] = True

    def _inject_std_change(self, series, params):
        """
        Inyecta un cambio en la desviación estándar (std_change) en las series.
        :param series: DataFrame con las series de tiempo.
        :param params: Diccionario con parámetros para la anomalía.
        """
        series_ids = params.get("series", [])
        start_point = params.get("start_point", 0)
        n_std = params.get("n_std", 1)

        # Asegúrate de que exista la columna "Anomaly"
        if "Anomaly" not in series.columns:
            series["Anomaly"] = False

        for series_id in series_ids:
            std = np.std(series.iloc[:, series_id])
            series.iloc[start_point:, series_id] += n_std * std
            
            # Marcar las filas afectadas como anomalías
            series.loc[start_point:, "Anomaly"] = True
