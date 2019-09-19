import pandas as pd
import gc
from nltk import sent_tokenize, word_tokenize, pos_tag

STANDARD_BATCH_SIZE = 10000

def tokenization(data, target,savePath = None, batchSize = STANDARD_BATCH_SIZE, nbRows = None):
    print("Initializing Tokenization.")

    if isinstance(data, pd.DataFrame):
        print("Using Data Frame.")
        data[str(target)] = data[str(target)].map(sent_tokenize)
        data[str(target)] = data[str(target)].map(lambda titles: 
            [
                word_tokenize(title) for title in titles
            ])
        return data

    elif isinstance(data, str):
        if isinstance(savePath, str):

            print("Importing file from path: {path}".format(path = str(data)))
            print("Saving file on path: {path}".format(path = str(savePath)))
            FtIndex = True
            for batch in pd.read_csv(str(data), chunksize=batchSize, nrows=nbRows):
                batch[str(target)] = batch[str(target)].map(sent_tokenize)
                batch[str(target)] = batch[str(target)].map(lambda titles: 
                    [
                        word_tokenize(title) for title in titles
                    ])
                
                batch.to_csv(str(savePath),
                                index = False,
                                header = FtIndex,
                                mode = "a"
                                    )
                if FtIndex:
                    FtIndex = False

                del(batch)
                gc.collect()
        else:
            processedData = []
            print("Importing file from path: {path}".format(path = str(data)))

            for batch in pd.read_csv(str(data), chunksize=batchSize, nrows=nbRows):
                batch[str(target)] = batch[str(target)].map(sent_tokenize)
                batch[str(target)] = batch[str(target)].map(lambda titles: 
                    [
                        word_tokenize(title) for title in titles
                    ])
                processedData.append(batch)

                del(batch)
                gc.collect()
            finalData = pd.concat(processedData, ignore_index = True)
            return finalData

    else:
        raise Exception("Data Format not supported. ",
            "Please input a Pandas DataFrame or a path for a 'csv' file.")
    
    print("Tokenization finished.")
