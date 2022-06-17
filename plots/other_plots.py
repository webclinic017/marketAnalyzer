

import statsmodels.api as sm
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib import cm
import pandas as pd
import numpy as np
def distribucion_univariante(serie,titulo):
    sns.displot(x=serie, height=10, bins=60, stat="frequency",kind="hist",color="green")
    plt.title(titulo)
    plt.figure()
    ax = sns.boxplot(y=serie, color="green")
    plt.title(titulo)
    plt.show()
    plt.figure()
    sm.qqplot(serie, line='45')
    plt.title(titulo)

def grouped_bar_plots(dataframe, groups):
    """

    :param dataframe:
    :type dataframe: pandas.DataFrame
    :param groups: nombre columna del dataframe para usar como eje x
    :type groups: str
    """
    width=0.15
    fig, ax = plt.subplots()
    df_aux=dataframe.transpose()
    df_aux.columns=df_aux.loc[groups]
    df_aux=df_aux.drop(groups)
    labels=dataframe.columns
    arrays=[]
    rects=[]
    labels = df_aux.index
    x = np.arange(len(labels))
    for j,i in enumerate(df_aux.columns):
        arrays.append(df_aux[i])
        rects.append(ax.bar(x - width / 2+j*width, df_aux[i], width, label=i))

    ax.set_ylabel('Scores')
    ax.set_title(groups)
    ax.set_xticks(x, labels)
    ax.legend()
    for rect in rects:
        ax.bar_label(rect, padding=3)

    fig.tight_layout()
    plt.show()

if __name__=="__main__":
    dataframe=pd.DataFrame({"clase":["uno","dos","tres"],"valor1":[1,2,5],"valor2":[3,4,10]})
    grouped_bar_plots(dataframe, "clase")




