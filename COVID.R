library(RODBC)
library(lubridate)
library(tidyverse)
library(magrittr)
library(janitor)
library(openxlsx)
library(taskscheduleR)
library(data.table)
library(padr)
library(zoo)
library(DT)
library(formattable)
library(tableHTML)
library(gridExtra)
library(kableExtra)
library(magick)

setwd("H:/R/Automated/Weekly_COVID")

cn <- odbcConnect("JupiterProd")

## Combines all WCM together, all LMH toghter
EDVisits <- sqlQuery(cn, "select distinct ed.EncounterNum
, case when ed.EDSite like 'Cornell%' then 'Cornell ED' else 'LMH ED' end as 'ED Site'
, EDArrivalDtm
, ed.EDSite
, SummaryDispoCategory


from Dashboards.ed.ed ed

where ed.EDSite in ('Cornell ED','Cornell Peds ED','LMH ED','LMH Peds ED')
and edarrivaldtm >= '2020-03-08'", as.is = T, stringsAsFactors =F)

EDVisits$`Day` = as.Date(EDVisits$EDArrivalDtm)

## All ED patients with COVID test
cn <- odbcConnect("JupiterProd")

COVID_Tests <- sqlQuery(cn, "select *

from (select distinct ed.EncounterNum
, ed.MRN
, ed.FirstName + ' ' + ed.LastName as 'Name'
, ed.Age
, case when ed.EDSite like 'Cornell%' then 'Cornell ED' else 'LMH ED' end as 'ED Site'
, Loc3 as 'Location'
, DATEADD(HOUR, datediff(hour, 0 , EDArrivalDtm), 0) as 'ED Arrival'
, EDArrivalDtm
, ordername
, ed.EDSite
, SummaryDispoCategory
, ValueAttribute as 'MD Name'
, ROW_NUMBER() over (partition by ed.encounternum order by ordercreatedwhen) as RN
, covid.ObservationValue as 'Result'


from Dashboards.ed.ed ed
inner join jupiter.scm.orders o (nolock) on o.Account = cast(ed.encounternum as varchar)
		and OrderName in ( 'microbiology sendout', 'SARS-CoV-2 RT-PCR')
left join dashboards.ed.ED_Dimensions dim with (nolock) on dim.EncounterNum = ed.EncounterNum 
			and valuename= 'ED Attending'
			and dim.RowNum = 1
left join JupiterCustomData.dbo.COVID19 covid (nolock) on cast(covid.EMPI as float)=cast( o.EMPI as float)

where ed.EDSite in ('Cornell ED','Cornell Peds ED','LMH ED','LMH Peds ED')
and edarrivaldtm >= '2020-03-08'

) a 

where RN  = 1
order by EDArrivalDtm", as.is = T, stringsAsFactors = F)

COVID_Tests$`Age Group` = ifelse(COVID_Tests$Age<= 21, '0 to 21',
                                 ifelse(COVID_Tests$Age > 21 & COVID_Tests$Age<=49, '22 to 49',
                                        ifelse(COVID_Tests$Age > 49 & COVID_Tests$Age<= 64, '50 to 64',
                                               '65 & older')))

COVID_Tests$`Day` <- as.Date(COVID_Tests$EDArrivalDtm)

## All MICU consults
cn <- odbcConnect("JupiterProd")


MICU_Consults <- sqlQuery(cn, "select distinct EncounterNum
, [MICU Consult Order]

from (select distinct ed.EncounterNum
, row_number() over (partition by encounternum order by ordercreatedwhen) as RN
, OrderCreatedWhen as 'MICU Consult Order'


from Dashboards.ed.ed ed
inner join jupiter.scm.Orders o (nolock) on o.Account = cast(ed.encounternum as varchar)
	and OrderName in ( 'ED Consult Order - MICU', 'ED Consult Order - MICU (LMH)')
	and OrderActive = 1

where EDSite in ('Cornell ED','Cornell Peds ED','LMH ED','LMH Peds ED')
and EDArrivalDtm >= '2020-03-01 12:00am') a

where RN = 1", as.is = T, stringsAsFactors = F)

## Missing 1 medication that Dr. Park is looking for
cn <- odbcConnect("JupiterProd")

Medication <- sqlQuery(cn, "select distinct EncounterNum
, OrderName as 'Medication'
, OrderCreatedWhen as 'Medication Order Time'

from (select distinct EncounterNum
, OrderName
, OrderCreatedWhen
, row_number () over (partition by encounternum order by ordercreatedwhen) as RN


from Dashboards.ed.ed ed 
inner join jupiter.scm.orders o (nolock) on o.Account = cast(ed.encounternum as varchar)
and ordername in (
'Hydroxychloroquine (PLAQUENIL) tablet +R+'
, 'Hydroxychloroquine Oral Susp'
, 'Hydroxychloroquine Oral'
, 'chg tocilizumab 20 mg/mL inj *R* 20 mL'
, 'Tocilizumab inj (NC) +R+'
, 'chg tocilizumab 20 mg/mL inj *R* - 4 mL'
, 'Tocilizumab inj +R+')
and OrderActive = 1

where EDSite in ('Cornell ED','Cornell Peds ED','LMH ED','LMH Peds ED')
and EDArrivalDtm >= '2020-03-01 12:00am')

a where RN = 1", as.is = T, stringsAsFactors = F)

# Add inpatient data -- starting point is endorsement. If endorsement not present use bed request or admit aware time
# if both not present use ED Arrival
# patients with "null" are still in the hospital

cn <- odbcConnect("JupiterProd")

Inpatient_Info <- sqlQuery(cn, "SET NOCOUNT ON
drop table if exists #admitted
select distinct EncounterNum

into #admitted
from Dashboards.ed.ed ed

where EDSite in ('Cornell ED','Cornell Peds ED','LMH ED','LMH Peds ED')
and EDArrivalDtm >= '2020-03-01 12:00am'
and SummaryDispoCategory = 'admitted'

drop table if exists #endorsement
select distinct p.EncounterNum
, p.endorsement
, ROW_NUMBER () over (partition by p.encounternum order by endorsement DESC) RN


into #endorsement
from (select distinct p.encounternum
, case when ObsCreatedWhen is not null then ObsCreatedWhen else o2.OrderCreatedWhen end as Endorsement


from #admitted p
--inner join Dashboards.ed.ed ed (nolock) on ed.EncounterNum = p.EncounterNum
left join jupiter.scm.Observations  o (nolock) on o.Account= cast(p.encounternum as varchar)
	and docName  in ( 'ED Disposition Note' , 'ED Unified Attending Note')
	and ObsName in ('ED SBAR md Ft', 'ED SBAR pager nu')
left join jupiter.scm.orders o2 (nolock) on o2.Account = cast(p.EncounterNum as varchar)
	and ordername = 'ED Bed Request/Admit to Hospital (cornell)' 
	) p

	--------------------- for patients without discharge order used discharge time
drop table if exists #exithospital
select distinct e.encounternum
, e.Endorsement
, discharge_time
, ROW_NUMBER () over (partition by e.encounternum order by discharge_time) as RN

into #exithospital

from (
select e.encounternum
, e.Endorsement
, coalesce(ordercreatedwhen, ed.visitdischargedtm) as discharge_time

from #endorsement e
left join jupiter.scm.orders o (nolock) on o.account = cast(e.encounternum as varchar)
		and ordername= 'Discharge Patient'
left join Dashboards.ed.ed ed (nolock) on e.EncounterNum = ed.EncounterNum
where RN = 1) e

---------
SET NOCOUNT OFF
select *
, DATEDIFF(hour, Endorsement,[Exit Hospital]) as 'Inpatient LOS hrs'
from (select distinct e.EncounterNum
, EDArrivalDtm
, case when Endorsement is null then coalesce( ed.AdmitAwareDtm, ed.AdmitDecisionDtm, ed.edarrivaldtm)
else Endorsement
end as 'Endorsement'
, discharge_time as 'Exit Hospital'


from #exithospital e
inner join Dashboards.ed.ed ed (nolock) on ed.EncounterNum = e.EncounterNum) a", as.is = T, stringsAsFactors = F)

# Add Intubation Code
cn <- odbcConnect("JupiterProd")

Intubation <- sqlQuery(cn, "select distinct EncounterNum

from (select DATEPART(day, edarrivaldtm) as Day
, ed.MRN
, EncounterNum
, ROW_NUMBER() over (partition by ed.encounternum order by doccreatedwhen) as RN
, DocName

from Dashboards.ed.ed ed
inner join jupiter.scm.Documents d (nolock) on d.Account =cast(ed.encounternum as varchar)
		and d.DocName in ('ED Procedure Note: endotracheal intubation'
		, 'Anesthesia Endotracheal Intubation/Consultation'
		,'Intubation Procedure Note')
		and d.DocIsCanceled = 0

where EDSite in ('Cornell ED','Cornell Peds ED','Cornell Psych ED')
and EDArrivalDtm >= '2020-03-01') a

where RN =1", as.is = T, stringsAsFactors= F)

# Add different ICUs code
cn <- odbcConnect("JupiterProd")

ICU <- sqlQuery(cn, "select distinct EncounterNum
, oud.Value as 'ICU'

from Dashboards.ed.ed ed
left join jupiterscm.JupiterSCM.CV3Order_East o (nolock) on o.ChartGUID = ed.ChartGUID and o.ImportSource = 'E'
inner join JupiterSCM.JupiterSCM.CV3OrderCatalogMasterItem_East  ocmi (nolock) on o.OrderCatalogMasterItemGUID = ocmi.GUID 
		and ocmi.Name in ('ED Bed Request/Admit to Hospital (cornell)', 'ED Bed Request/Admit to Hospital (LMH)') 
		and ocmi.ImportSource = 'E'
inner join jupiterscm.JupiterSCM.CV3OrderUserData_East oud  (nolock) on oud.OrderGUID = o.GUID and oud.ImportSource = 'E'
		and oud.value = 'ICU'

where ed.EDSite in ('Cornell Peds ED', 'Cornell ED', 'LMH ED','LMH Peds ED')
and EDArrivalDtm >=  '2020-03-01 12:00 am' ", as.is = T, stringsAsFactors =F)

# Add comorbidity code


COVID_Tests_Updated <- left_join(left_join(left_join(COVID_Tests, 
                                                     MICU_Consults, by = 'EncounterNum'),
                                           Medication, by = 'EncounterNum'),
                                 ICU, by = 'EncounterNum')

COVID_Positive <- COVID_Tests_Updated %>% 
  filter(Result == 'Detected')

COVID_Negative <- COVID_Tests_Updated %>% 
  filter(Result == 'Not Detected')

COVID_Pending <- COVID_Tests_Updated %>% 
  filter(Result == 'Pending')

##ED Arrivals by Site

plot_EDSite <- function(EDVisits, Site){
  
  EDSite <- EDVisits %>%
    filter(EDSite == Site) %>%
    select(EDSite) %>%
    unique() %>%
    .$EDSite
  
  
  return(EDVisits %>%
           filter(EDSite == Site) %>%
           group_by(Day) %>%
           summarise("Number of Arrivals" = n()) %>% 
           gather('EDSite', 'Value', -`Day`) %>% 
           ggplot(aes(x =  Day, y= Value, group = EDSite, fill = EDSite))+
           geom_point(aes(fill = EDSite, color = EDSite)) +
           geom_text(aes(color = EDSite, label = Value), position = position_nudge(y = +0.7)) +
           geom_line(aes(color = EDSite), size = 1) +
           scale_y_continuous(breaks = seq(0,30,1)) +
           theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
                 panel.background = element_blank(), axis.line = element_line(colour = "black"), axis.text.x = element_text(angle = 90, hjust = 1),
                 axis.text = element_text(face = "bold", color = "black"))+
           xlab(NULL) +
           ylab("Number of Patients")+
           ggtitle(paste0("Number of Arrivals by Day: ", Site)) +
           scale_color_manual(values = c("#CC5500","#708238")))
  
}  


for (Site in unique(EDVisits$EDSite)){
  p <- plot_EDSite(EDVisits, Site)
  
  ggsave( paste0("H:/R/Automated/Weekly_COVID/", Site, ".png"), p)
} 


## PLot ed admissions by day by site
##ED Arrivals by Site

plot_EDSite_Admit <- function(EDVisits, Site){
  
  EDSite <- EDVisits %>%
    filter(EDSite == Site) %>%
    select(EDSite) %>%
    unique() %>%
    .$EDSite
  
  
  return(EDVisits %>%
           filter(EDSite == Site) %>%
           filter(SummaryDispoCategory == 'Admitted') %>% 
           group_by(Day) %>%
           summarise("Number of Admissions" = n()) %>% 
           gather('EDSite', 'Value', -`Day`) %>% 
           ggplot(aes(x =  Day, y= Value, group = EDSite, fill = EDSite))+
           geom_point(aes(fill = EDSite, color = EDSite)) +
           geom_text(aes(color = EDSite, label = Value), position = position_nudge(y = +0.7)) +
           geom_line(aes(color = EDSite), size = 1) +
           scale_y_continuous(breaks = seq(0,30,1)) +
           theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
                 panel.background = element_blank(), axis.line = element_line(colour = "black"), axis.text.x = element_text(angle = 90, hjust = 1),
                 axis.text = element_text(face = "bold", color = "black"))+
           xlab(NULL) +
           ylab("Number of Patients")+
           ggtitle(paste0("Number of Admissions by Day: ", Site)) +
           scale_color_manual(values = c("#CC5500","#708238")))
  
}  


for (Site in unique(EDVisits$EDSite)){
  p <- plot_EDSite_Admit(EDVisits, Site)
  
  ggsave( paste0("H:/R/Automated/Weekly_COVID/Admissions", Site, ".png"), p)
} 


### COVID
Days <- EDVisits %>% 
  select(Day) %>% 
  unique()

###.	Number of ED patients tested for COVID

Test_n <- COVID_Tests %>% 
  group_by(`ED Site`, Day) %>% 
  summarise("Number of Tests" = n())

Test_WCM <- Test_n %>% 
  filter(`ED Site` == 'Cornell ED') %>% 
  ungroup() %>% 
  select(Day, `Number of Tests`)

Test_LMH <- Test_n %>% 
  filter(`ED Site` == 'LMH ED')%>% 
  ungroup() %>% 
  select(Day, `Number of Tests`)

Test_Sites <- left_join(left_join(Days,
                                  Test_WCM, by = 'Day' ),
                        Test_LMH, by = 'Day')
setnames(Test_Sites, old = c("Number of Tests.x", "Number of Tests.y"),
         new = c("Cornell EDs", "LMH EDs"))

Test_Sites[is.na(Test_Sites)] <- 0

Test_Sites %>% 
  gather('EDSite', 'Value', -`Day`) %>% 
  ggplot(aes(x =  Day, y= Value, group = EDSite, fill = EDSite))+
  geom_point(aes(fill = EDSite, color = EDSite)) +
  geom_text(aes(color = EDSite, label = Value), position = position_nudge(y = +0.7)) +
  geom_line(aes(color = EDSite), size = 1) +
  scale_y_continuous(breaks = seq(0,70,5)) +
  theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        panel.background = element_blank(), axis.line = element_line(colour = "black"), axis.text.x = element_text(angle = 90, hjust = 1),
        axis.text = element_text(face = "bold", color = "black"))+
  xlab(NULL) +
  ylab("Number of Patients")+
  ggtitle("Number of COVID Tests by Day by ED Site") +
  scale_color_manual(values = c("#CC5500","#708238"))


###.	Number of ED patients resulting +ve for COVID
Positive_n <- COVID_Positive %>% 
  group_by(`ED Site`, Day) %>% 
  summarise("Number of Positives" = n())

Positive_WCM <- Positive_n %>% 
  filter(`ED Site` == 'Cornell ED') %>% 
  ungroup() %>% 
  select(Day, `Number of Positives`)

Positive_LMH <- Positive_n %>% 
  filter(`ED Site` == 'LMH ED')%>% 
  ungroup() %>% 
  select(Day, `Number of Positives`)

Positive_Sites <- left_join(left_join(Days,
                                      Positive_WCM, by = 'Day' ),
                            Positive_LMH, by = 'Day')
setnames(Positive_Sites, old = c("Number of Positives.x", "Number of Positives.y"),
         new = c("Cornell EDs", "LMH EDs"))

Positive_Sites[is.na(Positive_Sites)] <- 0

# Into Line graph

Positive_Sites %>% 
  gather('EDSite', 'Value', -`Day`) %>% 
  ggplot(aes(x =  Day, y= Value, group = EDSite, fill = EDSite))+
  geom_point(aes(fill = EDSite, color = EDSite)) +
  geom_text(aes(color = EDSite, label = Value), position = position_nudge(y = +0.7)) +
  geom_line(aes(color = EDSite), size = 1) +
  scale_y_continuous(breaks = seq(0,30,1)) +
  theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        panel.background = element_blank(), axis.line = element_line(colour = "black"), axis.text.x = element_text(angle = 90, hjust = 1),
        axis.text = element_text(face = "bold", color = "black"))+
  xlab(NULL) +
  ylab("Number of Patients")+
  ggtitle("Number of COVID + by Day by ED Site") +
  scale_color_manual(values = c("#CC5500","#708238"))

ggsave("COVID_Positives_Day.png")


###	Positives/Negative: Number of ED patients admitted
Positive_admit <- COVID_Positive %>% 
  filter(SummaryDispoCategory == 'Admitted') %>% 
  group_by(`ED Site`, Day) %>% 
  summarise("Number of + Admissions" = n())

Admit_WCM <- Positive_admit %>% 
  filter(`ED Site` == 'Cornell ED')%>% 
  ungroup() %>% 
  select(Day, `Number of + Admissions`)

Admit_LMH <- Positive_admit %>% 
  filter(`ED Site` == 'LMH ED')%>% 
  ungroup() %>% 
  select(Day, `Number of + Admissions`)


Positive_Admit_Sites <- left_join(left_join(Days,
                                            Admit_WCM, by = 'Day' ),
                                  Admit_LMH, by = 'Day')
setnames(Positive_Admit_Sites, old = c("Number of + Admissions.x", "Number of + Admissions.y"),
         new = c("Cornell EDs", "LMH EDs"))

Positive_Admit_Sites[is.na(Positive_Admit_Sites)] <- 0

# Into Line graph

Positive_Admit_Sites %>% 
  gather('EDSite', 'Value', -`Day`) %>% 
  ggplot(aes(x =  Day, y= Value, group = EDSite, fill = EDSite))+
  geom_point(aes(fill = EDSite, color = EDSite)) +
  geom_text(aes(color = EDSite, label = Value), position = position_nudge(y = +0.7)) +
  geom_line(aes(color = EDSite), size = 1) +
  scale_y_continuous(breaks = seq(0,30,1)) +
  theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        panel.background = element_blank(), axis.line = element_line(colour = "black"), axis.text.x = element_text(angle = 90, hjust = 1),
        axis.text = element_text(face = "bold", color = "black"))+
  xlab(NULL) +
  ylab("Number of Patients")+
  ggtitle("Number of COVID Positives Admissions by Day by ED Site") +
  scale_color_manual(values = c("#CC5500","#708238"))

ggsave("COVID_Positives_Admissions_Day.png")



Negative_admit <- COVID_Negative %>% 
  filter(SummaryDispoCategory == 'Admitted') %>% 
  group_by(`ED Site`, Day) %>% 
  summarise("Number of + Admissions" = n())

Negative_Admit_WCM <- Negative_admit %>% 
  filter(`ED Site` == 'Cornell ED')%>% 
  ungroup() %>% 
  select(Day, `Number of + Admissions`)


Negative_Admit_LMH <- Negative_admit %>% 
  filter(`ED Site` == 'LMH ED')%>% 
  ungroup() %>% 
  select(Day, `Number of + Admissions`)



Negative_Admit_Sites <- left_join(left_join(Days,
                                            Negative_Admit_WCM, by = 'Day' ),
                                  Negative_Admit_LMH, by = 'Day')
setnames(Negative_Admit_Sites, old = c("Number of + Admissions.x", "Number of + Admissions.y"),
         new = c("Cornell EDs", "LMH EDs"))

Negative_Admit_Sites[is.na(Negative_Admit_Sites)] <- 0

# Into Line graph

Negative_Admit_Sites %>% 
  gather('EDSite', 'Value', -`Day`) %>% 
  ggplot(aes(x =  Day, y= Value, group = EDSite, fill = EDSite))+
  geom_point(aes(fill = EDSite, color = EDSite)) +
  geom_text(aes(color = EDSite, label = Value), position = position_nudge(y = +0.7)) +
  geom_line(aes(color = EDSite), size = 1) +
  scale_y_continuous(breaks = seq(0,30,1)) +
  theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
        panel.background = element_blank(), axis.line = element_line(colour = "black"), axis.text.x = element_text(angle = 90, hjust = 1),
        axis.text = element_text(face = "bold", color = "black"))+
  xlab(NULL) +
  ylab("Number of Patients")+
  ggtitle("Number of COVID Negative Admissions by Day by ED Site") +
  scale_color_manual(values = c("#CC5500","#708238"))

ggsave("COVID_Negatives_Admissions_Day.png")

###  .	ED LOS for patients admitted to floor
COVID_Tests_Admissions <- left_join(COVID_Tests_Updated %>% filter(SummaryDispoCategory == 'Admitted'),
                                    Inpatient_Info %>% select(EncounterNum, `Inpatient LOS hrs`), by = 'EncounterNum')

