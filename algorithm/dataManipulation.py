import pandas as pd

def featureExtraction(data, target: str, unique = True, columnName: str = None):
    """ Extracts a feature of a dataFrame. It can extract the pure features
    or filter by unique values

        Parameters
        ----------
        data : DataFrame
            The pandas Data Frame to be used in the feature extraction
        target : str
            
        columnName : str, optional
            The name of the column on the new generated feature file

        Return
        ------
            Pandas DataFrame. A dataFrame of the selected structure
    """

    # For future implementation of multiple feature extraction
    # if not isinstance(target, list):
    #     raise TypeError

    dataTmp = data[str(target)]
    if unique:
        dataTmp = dataTmp.unique()

    column = []
    if columnName is None:
        column.append(str(target))
    
    else:
        column.append(str(columnName))

    dataFinal = pd.DataFrame( dataTmp, columns = column )
    
    return dataFinal
