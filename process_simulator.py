from time_series_from_scratch import TimeSeriesSimulator
from time_series_analyzer import TimeSeriesAnalyzer
from anomaly_injector import AnomalyInjector  # Asegúrate de importar la clase que gestiona anomalías

class ProcessSimulator:
    def __init__(self):
        """
        Inicializa el simulador con las clases generadoras y analíticas.
        """
        self.generator = None
        self.analyzer = None

    def validate_config(self, config, mode):
        """
        Valida la configuración para garantizar que sea compatible con el modo seleccionado.
        """
        if mode == "from_scratch":
            required_keys = ["n_points", "n_series", "ar_params", "ma_params", "means", "stds", "corr_matrix"]
            missing_keys = [key for key in required_keys if key not in config]
            if missing_keys:
                raise ValueError(f"Faltan las siguientes claves en la configuración: {missing_keys}")

    def simulate_from_scratch(self, config):
        """
        Genera datos sintéticos desde cero con los parámetros definidos en la configuración.
        
        :param config: Diccionario con los parámetros para generar las series.
        :return: DataFrame con las series generadas.
        """
        # Crear instancia del generador
        self.generator = TimeSeriesSimulator(config)
        
        # Generar series ARMA
        series = self.generator.generate_arma_series()
        
        # Agregar tendencia si está definida en la configuración
        if "trend_slopes" in config:
            series = self.generator.add_trend(config["trend_slopes"])
        
        # Agregar estacionalidad si está definida en la configuración
        if "seasonality_periods" in config and "seasonality_amplitudes" in config:
            series = self.generator.add_seasonality(config["seasonality_periods"], config["seasonality_amplitudes"])
        
        return series

    def analyze_and_simulate(self, time_series, period=12, steps=500):
        """
        Analiza series de tiempo existentes y genera datos simulados basados en las características detectadas.
        
        :param time_series: DataFrame con las series de tiempo originales.
        :param period: Periodo estacional para la descomposición.
        :param steps: Número de pasos hacia adelante a simular.
        :return: DataFrame con las series extendidas simuladas.
        """
        # Crear instancia del analizador
        self.analyzer = TimeSeriesAnalyzer(time_series, period)
        
        # Descomponer las series
        self.analyzer.decompose()
        
        # Ajustar distribuciones a los residuos
        self.analyzer.fit_residual_distributions()
        
        # Generar extensión hacia adelante
        extended_series = self.analyzer.simulate_forward(steps)
        
        return extended_series

    def apply_anomalies(self, series, anomalies):
        """
        Aplica anomalías automáticamente según la configuración.
        
        :param series: DataFrame con las series generadas o analizadas.
        :param anomalies: Diccionario con la configuración de anomalías.
        :return: DataFrame con las series con anomalías aplicadas.
        """
        if anomalies:
            injector = AnomalyInjector(anomalies)
            series = injector.inject_anomalies(series)
        return series

    def simulate(self, mode, config=None, time_series=None, period=12, steps=500):
        """
        Punto de entrada principal para la simulación.
        """
        if mode == "from_scratch":
            if not config:
                raise ValueError("Se requiere un diccionario de configuración para generar datos desde cero.")
            series = self.simulate_from_scratch(config)
        elif mode == "analyze_and_simulate":
            if time_series is None:
                raise ValueError("Se requiere un DataFrame con series de tiempo para analizar y simular.")
            series = self.analyze_and_simulate(time_series, period, steps)
        else:
            raise ValueError("Modo inválido. Usa 'from_scratch' o 'analyze_and_simulate'.")

        # Validar y aplicar anomalías
        anomalies_config = config.get("anomalies", {})
        if not isinstance(anomalies_config, dict):
            print("Advertencia: No se proporcionaron anomalías válidas en la configuración.")
            anomalies_config = {}

        series = self.apply_anomalies(series, anomalies_config)
        return series

