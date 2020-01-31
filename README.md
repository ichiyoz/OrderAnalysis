# OrderAnalysis

step 1: run getSourceData.ipynb to generate CSV files

step 2: process generated files in step 1 by running ReadfromCSV.py into python dictionary format (json file)

step 3: generate analytical file by running createAnalyticalFile.ipynb

step 4: create sequences from step 2 using getV_fromCSV.py or CLIP pathway

step 5: run LCS_clustering.R for clustering

step 6: run stata StatAnalysis.do
