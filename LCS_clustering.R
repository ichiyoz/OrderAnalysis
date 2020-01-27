library(cluster)
library(TraMineR)
library(sparcl)
#use the printout from getV_fromCSV.py (line 332)
data=read.csv("/Users/yiyezhang/Dropbox/Grant-and-IRB/AHRQ R03/Analysis/EDDC_order_18_89_seq.csv",header=FALSE)

#data2=subset(data,V2==3)  #subset clusters
#N is len of data
data_seq=seqdef(data[,5:N],left="DEL",right = "DEL",gaps="DEL")  #look at first 4 events in the sequence

data_lcs <- seqdist(data_seq, method = "LCS")
clusterward_lcs <- agnes(data_lcs, diss = TRUE, method = "ward")
plot(clusterward_lcs)

cluster5_lcs <- cutree(clusterward_lcs, k =5)
table(cluster5_lcs)

#first column is first column of data, 2nd column is cluster number. 
write.csv(cluster5_lcs,'cluster.csv')
