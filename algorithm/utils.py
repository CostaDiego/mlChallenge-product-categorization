import pandas as pd
from os import path, makedirs

DEFAULT_COLUMN_NAME = 'Exist'

def checkDirs(dirs, create = False):
    if isinstance(dirs, list):
        print("Checkig the structures os directories.")
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
            if created:
                print("The following directories were created:\n{dirs}".format(
                    dirs = [dirs for dirs in created]))
            else:
                print("All directories already exists.")
        
        else:
            if nonexistent:
                print("The following directories are nonexistent:\n{dirs}".format(
                    dirs = [dirs for dirs in nonexistent]))
            else:
                print("All directories already exists.")

    else:
        raise Exception("The input must be an list")
    
def checkFiles(paths, files, column = DEFAULT_COLUMN_NAME):
    if isinstance(paths, list) and isinstance(files, list) and len(paths) == len(files) :
        indexName = []
        exists = []
        finalPath = []
        columnName = []

        columnName.append(str(column))

        for name in files:
            indexName.append(str(name))

        for i in range(len(paths)):
            finalPath.append(path.join(paths[i],files[i]))
            
        for pth in finalPath:
            exists.append(path.isfile(str(pth)))

        df = pd.DataFrame(exists, index = indexName, columns = columnName)
        return df

    else:
        raise Exception("The inputs must be lists and have the same lenght.")  