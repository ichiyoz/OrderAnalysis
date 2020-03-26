library(cluster)
library(TraMineR)
library(sparcl)
#use the printout from getV_fromCSV.py (line 332)
data=read.csv("/Users/yiyezhang/Dropbox/Grant-and-IRB/AHRQ R03/Analysis/EDDC_HF_2012_2018_MED_seq_cv3_notcancelled.csv",header=FALSE)
data=subset(data,V2=="train")
data_seq=seqdef(data[,3:23],left="DEL",right = "DEL",gaps="DEL",missing="")  #look at first 4 events in the sequence
data_lcs <- seqdist(data_seq, method = "LCS")
clusterward_lcs <- agnes(data_lcs, diss = TRUE, method = "ward")
plot(clusterward_lcs)
cluster5_lcs <- cutree(clusterward_lcs, k =4)
table(cluster5_lcs)
ColorDendrogram(clusterward_lcs, y = cluster5_lcs)

#first column is first column of data, 2nd column is cluster number. 
write.csv(cluster5_lcs,'cv3notcancelled.csv')
