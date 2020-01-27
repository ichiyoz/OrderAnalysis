import excel "/Users/yiyezhang/Dropbox/Grant-and-IRB/AHRQ R03/Analysis/EDDC_order_2012_2018_cluster.xlsx", sheet("EDDC_order_2012_2018_cluster") firstrow clear

keep if service=="MED"

encode marital, gen(nmarital)
encode gender, gen(sex)
encode langauge, gen(nlan)
encode race, gen(nrace)
encode ethnicity_guess, gen(nethnicity)
egen ncluster=group(cluster)

logit day CCW 1.cluster6 2.cluster6 3b.cluster6 4.cluster6 5.cluster6 6.cluster6 1.nrace 2.nrace 3.nrace 4.nrace 5b.nrace i.nlan i.sex YOB i.nmarital 1.nethnicity 2b.nethnicity 3.nethnicity , or
