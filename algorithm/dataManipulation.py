import pandas as pd
from os import path, makedirs

def featureExtraction(data, target: str, unique = True, columnName: str = None):
    """ Extracts a feature of a dataFrame. It can extract the pure features
    or filter by unique values

        Parameters
        ----------
        data : DataFrame
            The pandas Data Frame to be used in the feature extraction
        target : str
            The target column to extract features
        unique : bool, default True
            Inform whether or not is to filter the features by unique values
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

    dataFinal = pd.DataFrame( dataTmp, columns = column)
    
    return dataFinal

def filterPerRow(data, target, value, resetIndex = True, dropIndex = True):
    """Filter the row of the dataFrame by a given value.
    Implemented using the pandas DataFrame.

    Parameters
    ----------
    data: DataFrame
        The DataFrame intended to be filtered.
    target: str
        The target column to be filtered
    value: 
        The value to used in the filter

    return: DataFrame,
    ------
        Return a the filtered dataFrame
    """
    filteredData = data[data[str(target)] == str(value)]

    if resetIndex:
        filteredData = filteredData.reset_index(drop = dropIndex)

    return filteredData

def checkDirs(dirs, create = False):
    if isinstance(dirs, list):
        created = []
        nonexistent = []
        for directory in dirs:
            if not path.exists(str(directory)):
                if create:
                    makedirs(str(directory))
                    created.append(directory)

                else:
                    nonexistent.append(directory)
        
        if create:
            print("The following directories were created:\n{dirs}".format(dirs = [dirs for dirs in created]))
        
        else:
            print("The following directories are nonexistent:\n{dirs}".format(dirs = [dirs for dirs in nonexistent]))

    else:
        raise Exception("The input must be an list")
    
