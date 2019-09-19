import pandas as pd
import gc
from nltk import sent_tokenize, word_tokenize, pos_tag

STANDARD_BATCH_SIZE = 10000

def tokenization(data, target, batchSize = STANDARD_BATCH_SIZE, nbRows = None):
    print("Initializing Tokenization.")
    finalData = None
    if isinstance(data, pd.DataFrame):
        print("Using Data Frame.")
        data[str(target)] = data[str(target)].map(sent_tokenize)
        data[str(target)] = data[str(target)].map(lambda titles: 
            [
                word_tokenize(title) for title in titles
            ])
        finalData = data

    elif isinstance(data, str):
        processedData = []
        print("Importing file from path: {path}".format(path = str(data)))

        for batch in pd.read_csv(str(data), chunksize=batchSize, nrows=nbRows):
            data = batch
            data[str(target)] = data[str(target)].map(sent_tokenize)
            data[str(target)] = data[str(target)].map(lambda titles: 
                [
                    word_tokenize(title) for title in titles
                ])
            processedData.append(batch)

            del(data)
            gc.collect()
        finalData = pd.concat(processedData, ignore_index = True)

    else:
        raise Exception("Data Format not supported. ",
            "Please input a Pandas DataFrame or a path for a 'csv' file.")
    
    print("Tokenization finished.")

    return finalData