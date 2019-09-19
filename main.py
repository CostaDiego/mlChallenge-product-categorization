import pandas as pd
import gc
import os.path
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

print("Initializing script.")

print("Calling the method to perform the directories checks.")
dm.checkDirs(dirList, create = True)

print("Calling the tokenization method from dataProcessing library.")

print("Tokenizing portuguese dataset.")
dp.tokenization(
    os.path.join(TRAIN_DIR, TRAIN_PORTUGUESE_FILE),
    'title',
    savePath = os.path.join(TOKENS_DIR,TOKEN_PORTUGUESE_FILE),
    batchSize = 10000
)

print("Tokenizing spanish dataset.")
dp.tokenization(
    os.path.join(TRAIN_DIR, TRAIN_SPANISH_FILE),
    'title',
    savePath = os.path.join(TOKENS_DIR,TOKEN_SPANISH_FILE),
    batchSize = 10000
)

print("Exiting script.")



