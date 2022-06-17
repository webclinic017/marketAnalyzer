# This is a sample Python script.

# Press Mayús+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


from functions.prophet import prophet_

# Press the green button in the gutter to run the script.
from prophet.plot import add_changepoints_to_plot
import matplotlib.pyplot as plt
def plot_fechas(data,fecha):
        data_plot = data.loc[data.ds > fecha]
        if "oil" in data_plot.columns:
                prophet_.plot_variable_to_predict(data_plot, columnas=["y", "oil"])

def plot_real_predicted_values(model, forecast, df_test, title='Forecast'):
    fig = model.plot(forecast)
    a = add_changepoints_to_plot(fig.gca(), model, forecast)
    datenow = df_test.iloc[0]['ds']

    plt.title(title, fontsize=12)
    plt.xlabel("Day", fontsize=12)
    plt.ylabel("Prize", fontsize=12)
    plt.axvline(datenow, color="k", linestyle=":")
    # Añadir puntos reales en rojo
    plt.scatter(df_test['ds'], df_test['y'], c='red')
    plt.tight_layout()
    plt.show()
def plot_forecast(forecast, title):
    figure = plt.figure(figsize=(15, 15))
    plt.plot(forecast.ds, forecast.y, marker="o", color="Blue")
    plt.plot(forecast.ds, forecast.yhat, marker="o", color="Orange")
    leyenda = ["Real", "Forecast"]
    plt.legend(leyenda)
    titulo = title
    plt.title(titulo)
    plt.show()
def plot_variable_to_predict(df, date_col='ds', columnas=None,title=None):


    for columna in columnas:
        if columna != "ds":
            plt.figure(facecolor='w', figsize=(8, 6))
            plt.plot(df[date_col], df[columna])
            if title is not None:
                plt.title(title)
            else:
                plt.title(columna)
            plt.tight_layout()
            plt.show()

def plot_separated(df,symbol,fecha_sep,marker="o"):
    plt.figure(figsize=(12,8))

    aux=df.loc[df.ds>fecha_sep]
    t=df.loc[df.ds<=fecha_sep]

    plt.plot(t.ds,t.y,c="green",marker=marker)
    plt.plot(t.ds, t.yhat,c="red",marker=marker)
    plt.plot(aux.ds,aux.y,c="blue",marker=marker)
    plt.plot(aux.ds, aux.yhat,c="orange",marker=marker)
    plt.title(symbol)
    plt.show()






# See PyCharm help at https://www.jetbrains.com/help/pycharm/
