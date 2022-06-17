
# Merging them together
import pandas as pd
from functools import reduce
def merge(array_dataframes_with_same_index,how="outer"):
    df = reduce(
        lambda left ,right: pd.merge(
            left,
            right,
            left_index=True,
            right_index=True,
            how=how,
            suffixes=('', '_drop')
        ),
        array_dataframes_with_same_index)

    # Dropping redundant date and duplicate columns
    dup_cols = [i for i in df.columns if "date" in i or "Date" in i or "_drop" in i]
    df = df.drop(dup_cols, axis=1)
    return df

def get_lookbacks_(serie,n):
    num=len(serie)
    array=[]
    l_aux=0
    indice_aux = serie.index[l_aux]
    array.append(indice_aux)
    for _ in range(n-1):
        l_aux = l_aux+ int((num-l_aux) / 2)
        indice_aux = serie.index[l_aux]
        array.append(indice_aux)

    print(array)
    return array



def separate_train_test(df, num_test_dates=10):
    if num_test_dates > df.shape[0]:
        num_test_dates = df.shape[0] / 2

    # Separamos en training y testing
    return df.loc[df.index[:-num_test_dates]], df.loc[df.index[-num_test_dates:]]
