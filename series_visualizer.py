import matplotlib.pyplot as plt

class SeriesVisualizer:
    @staticmethod
    def plot_series(series, title="Series Generadas"):
        """
        Grafica todas las series en una sola figura.
        :param series: DataFrame con las series a graficar.
        :param title: Título de la gráfica.
        """
        series.plot(figsize=(12, 6), title=title)
        plt.xlabel("Tiempo")
        plt.ylabel("Valores")
        plt.grid()
        plt.show()

    @staticmethod
    def plot_individual_series(series, base_title="Serie"):
        """
        Grafica cada serie en una figura separada.
        :param series: DataFrame con las series a graficar.
        :param base_title: Título base para cada gráfica.
        """
        for column in series.columns:
            plt.figure(figsize=(8, 4))
            plt.plot(series[column], label=column)
            plt.title(f"{base_title}: {column}")
            plt.xlabel("Tiempo")
            plt.ylabel("Valores")
            plt.legend()
            plt.grid()
            plt.show()