import pandas as pd
import gc
import os.path
from algorithm import utils
import algorithm.dataManipulation as dm
import algorithm.dataProcessing as dp

BASE_DATASET_DIR = "./data/dataset/base"
TRAIN_DIR = "./data/dataset/train"
TOKENS_DIR = "./data/preProcessedData/tokens"

dirList = [BASE_DATASET_DIR, TRAIN_DIR, TOKENS_DIR]

BASE_TRAIN_FILE = "train.csv"
TRAIN_PORTUGUESE_FILE = "trainPortuguese.csv"
TRAIN_SPANISH_FILE = "trainSpanish.csv"
TOKEN_PORTUGUESE_FILE = "tokensPortuguese.csv"
TOKEN_SPANISH_FILE = "tokensSpanish.csv"
CLEAN_TOKEN_PORTUGUESE_FILE = "cleanTokensPortuguese.csv"
CLEAN_TOKEN_SPANISH_FILE = "cleanTokensSpanish.csv"

paths = [BASE_DATASET_DIR,
    TRAIN_DIR,
    TRAIN_DIR,
    TOKENS_DIR,
    TOKENS_DIR,
    TOKENS_DIR,
    TOKENS_DIR]

files = [BASE_TRAIN_FILE,
    TRAIN_PORTUGUESE_FILE,
    TRAIN_SPANISH_FILE,
    TOKEN_PORTUGUESE_FILE,
    TOKEN_SPANISH_FILE,
    CLEAN_TOKEN_PORTUGUESE_FILE,
    CLEAN_TOKEN_SPANISH_FILE]

INFO_COLUMN_NAME = utils.DEFAULT_COLUMN_NAME

print("Initializing script.")

print("Calling the method to perform the directories checks.")
utils.checkDirs(dirList, create = True)

print("Getting files info.")
info = utils.checkFiles(paths, files)

if not info.at[TOKEN_PORTUGUESE_FILE, INFO_COLUMN_NAME] or not info.at[TOKEN_SPANISH_FILE, INFO_COLUMN_NAME]:

    print("Calling the tokenization method from dataProcessing library.")

    if not info.at[TOKEN_PORTUGUESE_FILE, INFO_COLUMN_NAME]:
        print("Tokenizing portuguese dataset.")
        dp.tokenization(
            os.path.join(TRAIN_DIR, TRAIN_PORTUGUESE_FILE),
            'title',
            savePath = os.path.join(TOKENS_DIR,TOKEN_PORTUGUESE_FILE),
            batchSize = 10000
        )

    if not info.at[TOKEN_SPANISH_FILE, INFO_COLUMN_NAME]:
        print("Tokenizing spanish dataset.")
        dp.tokenization(
            os.path.join(TRAIN_DIR, TRAIN_SPANISH_FILE),
            'title',
            savePath = os.path.join(TOKENS_DIR,TOKEN_SPANISH_FILE),
            batchSize = 10000
        )
else:
    print("The data were already tokenized")

if not info.at[CLEAN_TOKEN_PORTUGUESE_FILE, INFO_COLUMN_NAME] or not info.at[CLEAN_TOKEN_SPANISH_FILE, INFO_COLUMN_NAME]:
    
    print("Calling token cleaner method")

    if not info.at[CLEAN_TOKEN_PORTUGUESE_FILE, INFO_COLUMN_NAME]:
        print("cleaning portuguese dataset.")
        dp.removeNonAlpha(
            os.path.join(TOKENS_DIR, TOKEN_PORTUGUESE_FILE),
            'title',
            savePath = os.path.join(TOKENS_DIR,CLEAN_TOKEN_PORTUGUESE_FILE),
            batchSize = 10000
        )
    if not info.at[CLEAN_TOKEN_SPANISH_FILE, INFO_COLUMN_NAME]:
        print("cleaning spanish dataset.")
        dp.removeNonAlpha(
            os.path.join(TOKENS_DIR, TOKEN_SPANISH_FILE),
            'title',
            savePath = os.path.join(TOKENS_DIR,CLEAN_TOKEN_SPANISH_FILE),
            batchSize = 10000
        )
else:
    print("The tokenized data were already cleaned.")

print("Exiting script.")



