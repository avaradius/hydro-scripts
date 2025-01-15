import numpy as np
import pandas as pd
from statsmodels.tsa.arima_process import ArmaProcess

class TimeSeriesSimulator:
    def __init__(self, config):
        """
        Inicializa el generador con una configuración.
        :param config: Diccionario de configuración con parámetros para generar las series.
        """
        self.config = config
        self.n_series = config.get("n_series", 1)
        self.n_points = config.get("n_points", 100)

    @staticmethod
    def corr_to_cov(corr_matrix, stds):
        """
        Convierte una matriz de correlación en una matriz de covarianza.
        :param corr_matrix: Matriz de correlación.
        :param stds: Lista de desviaciones estándar.
        :return: Matriz de covarianza.
        """
        std_matrix = np.diag(stds)
        return np.dot(std_matrix, np.dot(corr_matrix, std_matrix))

    def generate_noise(self):
        """
        Genera ruido basado en una matriz de correlación y desviaciones estándar.
        """
        corr_matrix = self.config.get("corr_matrix", np.eye(self.n_series))
        stds = self.config.get("stds", [1] * self.n_series)
        cov_matrix = self.corr_to_cov(corr_matrix, stds)

        # print("Matriz de covarianza calculada:", cov_matrix)

        return np.random.multivariate_normal(np.zeros(self.n_series), cov_matrix, size=self.n_points)

    def generate_arma_series(self):
        """
        Genera series ARMA multivariadas con las configuraciones proporcionadas.
        """
        ar_params = self.config.get("ar_params", [[0]] * self.n_series)
        ma_params = self.config.get("ma_params", [[0]] * self.n_series)
        means = self.config.get("means", [0] * self.n_series)
        stds = self.config.get("stds", [1] * self.n_series)

        noise = self.generate_noise()
        # print("Ruido generado (primeras filas):\n", noise[:5])
        # print("Correlación del ruido:\n", np.corrcoef(noise, rowvar=False))

        series = []
        for i in range(self.n_series):
            ar = np.r_[1, -np.array(ar_params[i])]
            ma = np.r_[1, np.array(ma_params[i])]
            arma_process = ArmaProcess(ar, ma)
            serie = arma_process.generate_sample(nsample=self.n_points) + noise[:, i]
            # print("Correlación antes de normalizar:\n", np.corrcoef(noise, rowvar=False))
            # Si AR y MA son cero, usar el ruido directamente
            if np.all(np.array(ar_params[i]) == 0) and np.all(np.array(ma_params[i]) == 0):
                serie = noise[:, i] * stds[i] + means[i]  # Escalar y ajustar media
            else:
                serie = (serie - np.mean(serie)) / np.std(serie)  # Estandarizar
                serie = serie * stds[i] + means[i]  # Escalar y ajustar media
            # print("Correlación despues de normalizar:\n", np.corrcoef(noise, rowvar=False))
            series.append(serie)

        self.series = pd.DataFrame(np.array(series).T, columns=[f"Serie_{i+1}" for i in range(self.n_series)])
        return self.series

    def add_trend(self, slopes):
        if slopes is None:
            print("Advertencia: 'trend_slopes' es nulo. No se agregará tendencia a las series.")
        else:
            for i, slope in enumerate(slopes):
                self.series[f"Serie_{i+1}"] += slope * np.arange(len(self.series))
        return self.series

    def add_seasonality(self, periods, amplitudes):
        if periods is None or amplitudes is None:
            print("Advertencia: 'seasonality_periods' o 'seasonality_amplitudes' son nulos. No se agregará estacionalidad a las series.")
        else:
            for i, (period, amplitude) in enumerate(zip(periods, amplitudes)):
                self.series[f"Serie_{i+1}"] += amplitude * np.sin(2 * np.pi * np.arange(len(self.series)) / period)
        return self.series