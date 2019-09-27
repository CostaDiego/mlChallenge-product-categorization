import pandas as pd
import gc
from ast import literal_eval
from nltk import sent_tokenize, word_tokenize
from nltk.corpus import stopwords


STANDARD_BATCH_SIZE = 10000
TOKEN_MIN_LENGTH = 3

UNIT_MEASUREMENT = ['mm','m','cm','km','kg','g','mg','l','v','a','w','ah',
    'n','lb','atm','ml','cm2','cm3','m2','m3','t']

def tokenization(data, target, savePath = None, batchSize = STANDARD_BATCH_SIZE, nbRows = None):
    print("\tInitializing Tokenization.")

    if isinstance(data, pd.DataFrame):
        print("\tUsing Data Frame.")
        data[str(target)] = data[str(target)].map(sent_tokenize)
        data[str(target)] = data[str(target)].map(lambda titles: 
            [
                word_tokenize(title) for title in titles
            ])
        return data

    elif isinstance(data, str):
        if isinstance(savePath, str):

            print("\tImporting file from path: {path}".format(path = str(data)))
            print("\tSaving file on path: {path}".format(path = str(savePath)))
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
            print("\tImporting file from path: {path}".format(path = str(data)))

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
        raise Exception("\tData Format not supported. ",
            "Please input a Pandas DataFrame or a path for a 'csv' file.")
    
    print("\tTokenization finished.")

def cleaner(word, minLen = TOKEN_MIN_LENGTH):
    # print(word)
    if isinstance(word, str):
        word = word.lower()

        for i in word:
            if not i.isalpha():
                word = word.replace(i,"")

        if len(word) >= minLen:
            return word
        else:
            return ''
        
    else:
        raise Exception("\tInput expected is a String")

def removeNonAlpha(data, target, savePath = None, batchSize = STANDARD_BATCH_SIZE, nbRows = None):
    print("\tInitializing data cleaning")
    
    if isinstance(data, pd.DataFrame):
        print("\tUsing Data Frame.")
        data[str(target)] = data[str(target)].map(lambda sentences:[ 
                    [
                        cleaner(token) for token in sentence if not '' 
                    ]for sentence in sentences])

        return data
    
    elif isinstance(data, str):
        if isinstance(savePath, str):
            print("\tImporting file from path: {path}".format(path = str(data)))
            print("\tSaving file on path: {path}".format(path = str(savePath)))

            FtIndex = True

            for batch in pd.read_csv(str(data), converters={'title':literal_eval}, chunksize=batchSize, nrows=nbRows):
                batch[str(target)] = batch[str(target)].map(lambda sentences:[ 
                    [
                        cleaner(token) for token in sentence if not None 
                    ]for sentence in sentences])          
                
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
            cleanedData = []
            print("\tImporting file from path: {path}".format(path = str(data)))

            for batch in pd.read_csv(str(data), converters={'title':literal_eval}, chunksize=batchSize, nrows=nbRows):
                batch[str(target)] = batch[str(target)].map(lambda sentences:[ 
                    [
                        cleaner(token) for token in sentence if not None
                    ]for sentence in sentences])

                cleanedData.append(batch)

                del(batch)
                gc.collect()
            finalData = pd.concat(cleanedData, ignore_index = True)
            return finalData

    else:
        raise Exception("\tData Format not supported. ",
            "Please input a Pandas DataFrame or a path for a 'csv' file.")
# --------------------------Revisar código Abaixo

def getStopWords(language):
    words = stopwords.words(str(language)) + UNIT_MEASUREMENT
    return words

def removeStopWords(data, target, language, savePath = None, batchSize = STANDARD_BATCH_SIZE, nbRows = None):
    stopWords = getStopWords(language)  
    print("\tInitializing removal of StopWords.")

    if isinstance(data, pd.DataFrame):
        print("\tUsing Data Frame.")
        data[str(target)] = data[str(target)].map(
        lambda sentences: [ 
            [
                token for token in sentence if token and token not in stopWords
            ]
            for sentence in sentences if sentence])
        return data

    elif isinstance(data, str):
        if isinstance(savePath, str):

            print("\tImporting file from path: {path}".format(path = str(data)))
            print("\tSaving file on path: {path}".format(path = str(savePath)))
            FtIndex = True
            for batch in pd.read_csv(str(data), chunksize=batchSize, nrows=nbRows):
                batch[str(target)] = batch[str(target)].map(lambda sentences: [ 
                    [
                        token for token in sentence if token and token not in stopWords
                    ]
                    for sentence in sentences if sentence])
                
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
            print("\tImporting file from path: {path}".format(path = str(data)))

            for batch in pd.read_csv(str(data), chunksize=batchSize, nrows=nbRows):
                batch[str(target)] = batch[str(target)].map(lambda sentences: [ 
                    [
                        token for token in sentence if token and token not in stopWords
                    ]
                    for sentence in sentences if sentence])
                    
                processedData.append(batch)

                del(batch)
                gc.collect()
            finalData = pd.concat(processedData, ignore_index = True)
            return finalData

    else:
        raise Exception("\tData Format not supported. ",
            "Please input a Pandas DataFrame or a path for a 'csv' file.")
    
    print("\tRemoval of StopWords finished.")