library(cluster)
library(TraMineR)
library(sparcl)
data=read.csv("/Users/yiyezhang/Dropbox/Grant-and-IRB/AHRQ R03/Analysis/EDDC_order_18_89_seq.csv",header=FALSE)
data=read.csv("/Users/yiz2014/Desktop/ppdcluster.csv",header=FALSE)

data2=subset(data,V2==3)  #subset clusters
data_seq_2=seqdef(data2[,5:21],left="DEL",right = "DEL",gaps="DEL")  #look at first 4 events in the sequence
data_seq_2
mean(seqlength(data_seq_1))
datae=read.csv("/Users/yiz2014/Documents/Data/OMOP/CLEVER_diagpath_alldiag_DISCO_1.csv",header=FALSE)
data_seqe=seqecreate(datae,id=datae$V1,timestamp = datae$V2,event=datae$V4) #data_seqe=seqecreate(data_seq_1,tevent='state')
df.subseq <- seqefsub(data_seqe, min.support=0.8) # searching for frequent event subsequences
plot(df.subseq[1:10], col="cyan", ylab="Frequency", xlab="Subsequences", cex=1.5) # plotting
#fsubseq <- seqefsub(data_seqe, min.support = 0.1)

discrseq <- seqecmpgroup(df.subseq, group=data_1$V3) # searching for frequent sequences that are related to gender
head(discrseq)
#plot(discrseq[1:5], cex=1.5) # plotting 10 frequent subsequences
#plot(discrseq[1:5], ptype="resid", cex=1.5) # plotting 10 residuals

rules <- TraMineR:::seqerules(df.subseq)
head(rules)

#IGNORE BELOW
crt.labels <- c("below 0.5mg/dL", "0.5-1.2mg/dL", "1.2-1.6mg/dL","1.6-2.2mg/dL", "2.2-4.2mg/dL", "above 4.2mg/dL")
crt.scode <- c("be", "0", "12", "16", "2", "ab")
crt.seq <- seqdef(crt[3:6], right="DEL",gaps="DEL",left="DEL",states = crt.scode,labels = crt.labels, xtstep = 6)
seqdplot(hemo_seq, title = "Hemoglobin", withlegend = FALSE,border = NA)

data_lcs <- seqdist(data_seq_1, method = "LCS")
mean(data_lcs)
data_lcs <- seqdist(data_seq_1, method = "OMstran",sm="TRATE",indel=2)
write.csv(data_lcs, "/Users/yiz2014/Documents/Data/OMOP/CLEVER_diagpath_LCS.csv")

print(model.labels_)
libmean(data_lcs)
clusterward_lcs<-diana(data_lcs)
clusterward_lcs <- agnes(data_lcs, diss = TRUE, method = "ward")
plot(clusterward_lcs)


cluster5_lcs <- cutree(clusterward_lcs, k =5)
table(cluster5_lcs)
pdf(file="dendrogram.pdf", height=10, width=200)
ColorDendrogram(clusterward_lcs, y = cluster5_lcs, labels = data[,1])
dev.off()
cluster4_lcs <- factor(cluster4_lcs, labels = c("Type 1", "Type 2", "Type 3","Type 4"))

write.csv(cluster5_lcs,'PPD_cluster.csv')
eclusterk<-pam(data_lcs,5)

data_lcp <- seqdist(data_seq_1, method = "LCP")
clusterward_lcp <- agnes(data_lcp, diss = TRUE, method = "ward")
 

plot(clusterward_lcp)

cluster10_lcp <- cutree(clusterward_lcp, k = 10)
cluster10_lcp <- factor(cluster10_lcp, labels = c("Type 1", "Type 2", "Type 3","Type 4","Type 5","Type 6","Type 7","Type 8","Type 9","Type 10"))
table(cluster10_lcp)

si5 <- silhouette(cutree(clusterward_lcp, k = 5), daisy(data_lcp))

couts <- seqsubm(data_seq, method = "TRATE")
data_om=seqdist(data_seq_1, method = "OM", indel = 3, sm = couts)
clusterward_om <- agnes(data_om, diss = TRUE, method = "ward")
plot(clusterward_om)
cluster10_om <- cutree(clusterward_om, k = 2)
cluster10_om <- factor(cluster10_om, labels = c("Type 1", "Type 2"))
table(cluster10_om)

tr <- seqtrate(data_seq)
tr_round=round(tr, 2)

maxcol_type1=matrix(c(NA),nrow=dim(tr_type1)[1],ncol=3)
for (i in 1:dim(tr_type1)[2]){
  maxcol_type1[i,1]=rownames(tr_type1)[i]
  maxcol_type1[i,2]=names(which.max(tr_type1[i,]))
  maxcol_type1[i,3]=max(tr_type1[i,])
}
j=1
for (i in 1:dim(tr_type1)[2]){
  for (s in 1:dim(tr_type1)[1]){
    maxcol_type1[j,1]=rownames(tr_type1)[i]
    if (tr_type1[i,s]>0.3){
      maxcol_type1[j,2]=colnames(tr_type1)[s]
      maxcol_type1[j,3]=tr_type1[i,s]
      j=j+1
    }  
}}

for(i in 1:257) {
  assign(paste0("data_seq_", i), seqdef(data[i,-1],right="DEL",gaps="DEL"))
}

data_eseq=seqecreate(data_seq)
data.seq=seqdef(data,cpal=heat.colors(5))
data_C1=rbind(data[4,],data[6,],data[25,])
data_C1_seq=seqdef(data_C1[c(-1)],right="DEL",cpal=heat.colors(40))
similarity_table = matrix(c(NA),nrow=257,ncol=257)
for(i in 1:256) {
  for (j in (i+1):257){   
    similarity_table[i,j]=seqmpos(data_seq[i, ], data_seq[j, ])
}}
lcs_table=matrix(c(NA),nrow=257,ncol=257)
for(i in 1:256) {
  for (j in (i+1):257){   
    lcs_table[i,j]=seqLLCS(data_seq[i, ], data_seq[j, ])
  }}
lcp_table=matrix(c(NA),nrow=257,ncol=257)
for(i in 1:256) {
  for (j in (i+1):257){   
    lcs_table[i,j]=seqLLCP(data_seq[i, ], data_seq[j, ])
  }}

lcs=seqdist(data_seq, method = "LCS")
time.constraint <- seqeconstraint(maxGap = 2, windowSize = 10)
fsubseq <- seqefsub(data_eseq, pMinSupport = 0.8, constraint = time.constraint)
discrcohort <-seqecmpgroup(fsubseq, method = "bonferroni")

cost_mat <- seqsubm(data_seq, method = "TRATE")
data_om <- seqdist(data_seq, method = "OM", sm = cost_mat)
data_om_c=seqdist(data_seq, method = "OM", sm = ccost)
clusterward <- agnes(data_om, diss = TRUE, method = "ward")
plot(clusterward, which.plots = 2)
cluster3 <- cutree(clusterward, k = 3)
cluster3 <- factor(cluster3, labels = c("Type 1", "Type 2", "Type 3"))
table(cluster3)
seqmtplot(data_seq, group = cluster3)

data_transition=seqetm(data_seq, method = "period")

ml_pat=read.csv("/Users/ichiyoz/Dropbox/TMA project/DataProcessing/pt_race.csv",header=TRUE)
#with(ml_pat,table(type,allergy_y_n))
ml_pat$type1 <- relevel(ml_pat$type, ref = "Type 1")
test <- multinom(type1 ~ hyp_y_n+count_prc, data = ml_pat)
#summary(test)
z <- summary(test)$coefficients/summary(test)$standard.errors
p <- (1 - pnorm(abs(z), 0, 1))/2
dwrite <- data.frame(hyp_y_n = rep(c("Y", "N"), each =99), count_prc = rep(c(1:99),2 ))
pp.write <- cbind(dwrite, predict(test, newdata = dwrite, type = "probs", se = TRUE))
lpp <- melt(pp.write, id.vars = c("hyp_y_n", "count_prc"), value.name = "probability")
#head(lpp) 
ggplot(lpp, aes(x = count_prc, y = probability, colour = hyp_y_n)) + geom_line() + facet_grid(variable ~ ., scales = "free")