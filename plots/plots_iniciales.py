from utils.dataframes import work_dataframes
import matplotlib.pyplot as plt

def plot_serie_temporal(serie,titulo,column,num_plots=4,archivo=None,kind=None):
    """

    :param serie:
    :param titulo:
    :param column:
    :param num_plots:
    :param archivo:  archivo para guardar los plos
    :param kind: bar or None (tipo de grafico)
    """
    array_indices= work_dataframes.get_lookbacks_(serie, num_plots)
    figure,ax = plt.subplots(num_plots // 2, 2,figsize=(3*num_plots,2*num_plots))


    plt.subplots_adjust(left=0.1,
                        bottom=0.1,
                        right=0.9,
                        top=0.9,
                        wspace=0.4,
                        hspace=0.8)
    for k,i in enumerate(array_indices):


        serie_aux=serie.loc[serie.index>i]

        if kind=="bar":
            ax[k // 2, k % 2].bar(serie_aux.index, serie_aux[column])
        else:
            ax[k//2,k%2].plot(serie_aux.index,serie_aux[column])
        ax[k//2,k%2].set_title(titulo+" "+str(i.date()))
        for tick in   ax[k//2,k%2].get_xticklabels():
            tick.set_rotation(25)

    figure.show()
    if archivo is not None:
        figure.savefig(archivo)


def plot_daraframe_temporal(data,titulo,num_plots=4,archivo=None,kind=None):
    array_indices= work_dataframes.get_lookbacks_(data, num_plots)
    figure,ax = plt.subplots(num_plots // 2, 2,figsize=(3*num_plots,2*num_plots))




    plt.subplots_adjust(left=0.1,
                        bottom=0.1,
                        right=0.9,
                        top=0.9,
                        wspace=0.4,
                        hspace=0.3)
    data=data.dropna()
    dataframe=(data-data.iloc[0])/data.iloc[0]
    #dataframe=(data-data.mean())/data.std()
    for k,i in enumerate(array_indices):


        dataframe=dataframe.loc[dataframe.index>i]
        for column in dataframe.columns:
            if kind == "bar":
                ax[k // 2, k % 2].bar(dataframe.index,dataframe[column])
            else:
                ax[k//2,k%2].plot(dataframe.index,dataframe[column])
        ax[k//2,k%2].set_title(titulo+" "+str(i))
        for tick in   ax[k//2,k%2].get_xticklabels():
            tick.set_rotation(25)
        ax[k//2,k%2].legend(data.columns)

    figure.show()
    if archivo is not None:
        figure.savefig(archivo)

