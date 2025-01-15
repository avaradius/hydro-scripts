import numpy as np
import pandas as pd
from statsmodels.tsa.seasonal import seasonal_decompose
from fitter import Fitter
from scipy.stats import norm, lognorm, expon, uniform

class TimeSeriesAnalyzer:
    def __init__(self, time_series, period=12):
        """
        Inicializa el analizador con series de tiempo (univariadas o multivariadas).
        :param time_series: DataFrame o Series con las series de tiempo.
        :param period: Periodo estacional (por defecto 12).
        """
        self.data = pd.DataFrame(time_series)
        self.period = period
        self.results = {}
        self.trends = {}
        self.seasonals = {}
        self.residuals = {}
        self.best_distributions = {}

    def decompose(self):
        """
        Descompone cada serie de tiempo en tendencia, estacionalidad y residuo.
        """
        for column in self.data.columns:
            result = seasonal_decompose(self.data[column], model='additive', period=self.period)
            trend = result.trend.dropna()
            seasonal = result.seasonal.dropna()
            residual = result.resid.dropna()
            common_index = trend.index.intersection(seasonal.index).intersection(residual.index)

            self.results[column] = result
            self.trends[column] = trend.loc[common_index]
            self.seasonals[column] = seasonal.loc[common_index]
            self.residuals[column] = residual.loc[common_index]

    def fit_residual_distributions(self):
        """
        Ajusta distribuciones estadísticas para los residuos de cada serie.
        """
        for column, residual in self.residuals.items():
            fitter = Fitter(residual, distributions=['norm', 'lognorm', 'expon', 'uniform'])
            fitter.fit()
            self.best_distributions[column] = fitter.get_best()
            print(f"Mejor distribución ajustada para {column}: {self.best_distributions[column]}")

    def simulate_forward(self, steps=500):
        """
        Genera una extensión hacia adelante para cada serie de tiempo.
        :param steps: Número de pasos a generar hacia adelante.
        """
        simulated_series = {}

        for column in self.data.columns:
            trend = self.trends[column]
            seasonal = self.seasonals[column]
            residual = self.residuals[column]
            distribution = self.best_distributions[column]

            # Extender tendencia
            trend_slope = (trend.iloc[-1] - trend.iloc[0]) / len(trend)
            extended_trend = [trend.iloc[-1] + i * trend_slope for i in range(1, steps + 1)]

            # Extender estacionalidad
            extended_seasonal = list(seasonal.values) * (steps // self.period) + list(seasonal.values[:steps % self.period])
            extended_seasonal = extended_seasonal[:steps]

            # Generar residuos extendidos
            distribution_name = list(distribution.keys())[0]
            distribution_params = distribution[distribution_name]
            if distribution_name == "norm":
                extended_residual = norm.rvs(
                    loc=distribution_params["loc"], scale=distribution_params["scale"], size=steps
                )
            elif distribution_name == "lognorm":
                extended_residual = lognorm.rvs(
                    s=distribution_params["s"], loc=distribution_params["loc"], scale=distribution_params["scale"], size=steps
                )
            elif distribution_name == "expon":
                extended_residual = expon.rvs(
                    loc=distribution_params["loc"], scale=distribution_params["scale"], size=steps
                )
            elif distribution_name == "uniform":
                extended_residual = uniform.rvs(
                    loc=distribution_params["loc"], scale=distribution_params["scale"], size=steps
                )
            else:
                raise ValueError(f"Distribución '{distribution_name}' no soportada para simulación.")

            # Recombinar los componentes
            simulated_series[column] = (
                np.array(extended_trend) + np.array(extended_seasonal) + np.array(extended_residual)
            )

        return pd.DataFrame(simulated_series)