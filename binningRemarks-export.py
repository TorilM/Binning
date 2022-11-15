#!/usr/bin/env python
# coding: utf-8

# In[1]:



import pyodbc
import csv
import pandas as pd
print('kör')

dateFilter = "fel.Timestamp >= DATEADD(DAY, -360, GETDATE()) "

db = pyodbc.connect('DRIVER={SQL Server};database=Produktion;server=sed-eu-db-prodrep;uid=analys;pwd=!al2005!')
cursor = db.cursor()

fromString_float = "TraceabilitySystem.dbo.ProductRemarks as fel join TraceabilitySystem.dbo.Symptoms as s on                                        s.ID = fel.SymptomID join TraceabilitySystem.dbo.States as st on                                        st.ID = fel.ReportStateID join TraceabilitySystem.dbo.ProductSerials as p on                           p.ProductID = fel.ProductID left join TraceabilitySystem.dbo.Products as pr on                                 pr.id = fel.productid left join TraceabilitySystem.dbo.models as m on                                     m.id = pr.modelID left join TraceabilitySystem.dbo.RemarkCauseSolutions as atg on             atg.RemarkID = fel.ID left join TraceabilitySystem.dbo.ProductSerials as p2 on                    p2.ProductID = atg.oldcomponent left join TraceabilitySystem.dbo.Products as pr2 on                               pr2.id = p2.productid left join TraceabilitySystem.dbo.Solutions as sol on                              sol.ID = atg.SolutionID left join TraceabilitySystem.dbo.Causes as c on                                     c.ID = atg.CauseID " #"#join Produktion.dbo.PT_Main as main on                                  main.ProductName = p.Serial collate SQL_Latin1_General_CP1_CI_AS "
#join TraceabilitySystem.dbo.States as st2 on                                      st2.ID = pr.currentStateID \
#left join TraceabilitySystem.dbo.ProductRevisions as r on                    r.ProductID = p2.ProductID \
#left join TraceabilitySystem.dbo.models as m2 on                                   m2.id =  pr2.modelid \
#left join aspnetdb.dbo.aspnet_Users as u1 on                                   u1.UserId = atg.TroubleshooterID \
#join aspnetdb.dbo.aspnet_Users as u2 on                                        u2.UserId = fel.ReporterID \
#left join TraceabilitySystem.dbo.modelgroups as mg on                              mg.id = m.modelgroupid \

#SQL input parametrar
fieldsToSelect = """p.Serial [Unit], st.name as [st], fel.Timestamp [ErrorDate], s.symptom [Symptom], fel.Comment [SymptomComment], 
st.name as [ErrorState], u2.UserName as [Reporter],sol.Solution, p2.serial [ChangedSerial], m2.name [ChangedModel], 
atg.Solutioncomment, atg.timestamp [SolutionDate],
U1.UserName [Troubleshooter], atg.causecomment [CauseComment], c.cause [Cause], m.name [Model], pr.createdtimestamp [UnitCreated]"""
  
#######################################################
fråga="fel.Timestamp [ErrorDate], p.Serial as [Unit], st.name as [st],  s.symptom [Test], fel.Comment [SymptomComment], atg.Solutioncomment as [atgSolution], atg.causecomment as [CauseComment], c.cause as [Cause], sol.Solution as [Solution] "
#####################################################
sqlQuery = "select " +fråga+ " from " +fromString_float+ " where m.name ='SX10' and " +dateFilter+ " order by fel.Timestamp "  

cursor1 = db.execute(sqlQuery)
rows = cursor1.fetchall()

db.commit()
db.close()

remarks = pd.DataFrame(columns=["ErrorDate","Unit","Test"]) 
remarks["ErrorDate"] = [i[0] for i in rows]
remarks["Unit"]      = [i[1] for i in rows]
remarks['Cause']   = [i[7] for i in rows]
remarks["Test"]      = [i[3] for i in rows]
remarks["Fel"]       = [i[4] for i in rows]
remarks["atgSolution"]   = [i[5] for i in rows]
remarks["solutioncomment"]   = [i[6] for i in rows]
remarks["Solution"]   = [i[8] for i in rows]
#remarks["TTS-fel"]  = [i[4] for i in rows]
print('klart')

remarks=remarks.drop_duplicates(['ErrorDate','Unit'], keep='last')
remarks=remarks.reset_index(drop=True)

remarks.to_csv('C:/junk/remarks12mnd.csv' )
remarks.head(3)
#remarks.dtypes


# In[134]:


#försöker att kategoriesera felfall mha en csv-fil
import pandas as pd
remarks=pd.read_csv('C:/junk/remarks12mnd.csv', encoding='utf-8' )
remarks['felfall']='okänd'
felkod=pd.read_csv('C:/Sally presentationer/felkoder15.csv', sep=',', encoding='ISO-8859-1',quotechar='"' )
#felkod['Fel'].fillna('manuell', inplace=True)
sistaremarks=len(remarks)
sistafelkod=len(felkod)
print(sistafelkod)
remarknr=0
#sätter huvudgrupp
while remarknr<sistaremarks:
    TTS=remarks.iloc[remarknr,5];
    felnr=0
    if (isinstance(TTS, str)):
        while felnr<sistafelkod:
            long=felkod.iloc[felnr,0]   
            #print('felnr',felnr,'\t long=',long,'\t TTS=',TTS)
            if long in TTS:
                remarks.iloc[remarknr,9]=felkod.iloc[felnr,2]
                #print('ny felkod',felkod.iloc[felnr,1]) 
            felnr+=1
    remarknr+=1  
#sätter mindre grupp
remarks['detaljerad']='okänd'
remarknr=0
while remarknr<sistaremarks:
    TTS=remarks.iloc[remarknr,5];
    felnr=0
    if (isinstance(TTS, str)):
        while felnr<sistafelkod:
            long=felkod.iloc[felnr,0]   
            if long in TTS:
                remarks.iloc[remarknr,10]=felkod.iloc[felnr,1]
            felnr+=1
    remarknr+=1      
    
    
remarks.to_csv('C:/junk/felfall12mnd.csv' )
#remarks.iloc[156:173,:]
remarks.iloc[156:158,:]


# In[146]:


import pandas as pd
minafelfall=pd.read_csv('C:/junk/felfall12mnd.csv' )

#minafelfall.head(25)
minafelfall=minafelfall.groupby(["felfall","detaljerad","ErrorDate"],as_index=False).sum()
minafelfall['år'] = pd.DatetimeIndex(minafelfall['ErrorDate']).year
minafelfall['veckonummer'] = pd.to_datetime(minafelfall['ErrorDate'], errors='coerce').dt.week
minafelfall['månad'] = pd.DatetimeIndex(minafelfall['ErrorDate']).month
minafelfall['år-månad'] = pd.to_datetime(minafelfall['ErrorDate']).dt.to_period('M')
minafelfall.head()
minafelfall=minafelfall.drop(minafelfall.columns[4],axis=1)
minafelfall=minafelfall.drop(minafelfall.columns[3],axis=1)
minafelfall=minafelfall.drop(minafelfall.columns[2],axis=1)
minafelfall.to_csv('C:/junk' + '/felmedveckonummer12mnd.csv' )
minafelfall.head()


# In[147]:


#vill ha med noll
minafelfall=pd.read_csv('C:/junk/felmedveckonummer12mnd.csv' )
NYDF=minafelfall.pivot_table(index='veckonummer', 
                     columns=['felfall','detaljerad'],
                     
                     fill_value=0, 
                     aggfunc='count').unstack().to_frame('Antal').reset_index().rename(columns={0:'Action'})
#idx=pd.date
NYDF=NYDF.drop(NYDF.columns[0],axis=1)
NYDF.to_csv('C:/junk' + '/felpervecka12mnd.csv' )
NYDF.iloc[2370:2371,:]
#NYDF.tail()


# In[138]:


#antal levererade senaste året

db = pyodbc.connect('DRIVER={SQL Server};database=Produktion;server=sed-eu-db-prodrep;uid=analys;pwd=!al2005!')
cursor = db.cursor()

sqlQuery = "select (p.serial ) as [lag], MIN(t2.Timestamp) as [Slutdatum] from TraceabilitySystem.dbo.Products as pr join TraceabilitySystem.dbo.ProductSerials as p on pr.ID = p.ProductID join TraceabilitySystem.dbo.Models as m on m.ID = pr.modelID join TraceabilitySystem.dbo.ModelGroups as mg on m.ModelGroupID = mg.ID join TraceabilitySystem.dbo.Timings as t2 on t2.ProductID = p.ProductID join TraceabilitySystem.dbo.States as s2 on s2.ID = t2.StateID join TraceabilitySystem.dbo.StatesInWorkflows as w2 on s2.ID = w2.StateID join TraceabilitySystem.dbo.WorkFlows as wf on wf.ID = w2.ID and wf.ID = m.WorkFlowID where w2.ReportTag like 'End Prod' and mg.name = 'Scanning total station' group by p.serial having min(t2.Timestamp) >=DATEADD(DAY, -360, GETDATE()) "  

cursor1 = db.execute(sqlQuery)
rows = cursor1.fetchall()

db.commit()
db.close()

lev = pd.DataFrame(columns=[]) 
lev["ID"]            = [i[0] for i in rows]
lev["LevDatum"]      = [i[1] for i in rows]
lev['count']         =1
lev['år'] = pd.DatetimeIndex(lev['LevDatum']).year
lev['veckonummer'] = pd.to_datetime(lev['LevDatum'], errors='coerce').dt.week
lev['week'] = np.where(lev['veckonummer'] < 10,                       (lev['år'].astype(str).str[2:]+'-0'+lev['veckonummer'].astype(str)),                      lev['år'].astype(str).str[2:]+'-'+lev['veckonummer'].astype(str))
lev.to_csv('C:/Sally felsokning/lev.csv' )
Levererade=lev.pivot_table(index='week', 
                     #columns=['week'],
                     
                     fill_value=0, 
                     aggfunc='count').unstack().to_frame('Antal').reset_index().rename(columns={0:'Action'})
Levererade.head(3)
#blir lätt stora hopp när utleveranser varierar, lägger in ett rolling average
Lev['Mean'] = Lev['count'].rolling(window=5).mean().shift(-2)#.iloc[2:].values
Lev['Mean']=Lev['Mean'].ffill(axis=0)
Lev['Mean']=Lev['Mean'].bfill(axis=0)
#Lev=lev.groupby(["week"],as_index=False)['count'].sum()
#Lev['veckonummer']=Lev['week'].astype(str).str[3:]
Lev.to_csv('C:/Sally felsokning/Lev2.csv' )
Lev.head(5)


# In[127]:


Lev=pd.read_csv('C:/Sally felsokning/Lev2.csv' )

count=alt.Chart(Lev).mark_line().encode(
    alt.X('week', axis=alt.Axis(title='veckonummer ')),
    alt.Y('count:Q' ))
Mean=alt.Chart(Lev).mark_line(color='red').encode(
    alt.X('week', axis=alt.Axis(title='veckonummer ')),
    alt.Y('Mean:Q' ))
count+Mean


# In[151]:


#normera antal fel mot leveranser
Leveranser=pd.read_csv('C:/Sally felsokning/Lev2.csv')
Leveranser.head()
NYDF=pd.read_csv('C:/junk' + '/felpervecka12mnd.csv' )
DF=pd.merge(NYDF,Leveranser,
           on=['veckonummer'], how='outer')

DF=DF.drop(DF.columns[0],axis=1)
DF['Antal']=DF['Antal'].fillna(0)
DF['felfrekvens']=(DF['Antal']/DF['Mean']).fillna(0)
DF=DF.drop(['veckonummer','Unnamed: 0_y','Unnamed: 0.1'], axis=1)
DF.to_csv('C:/junk' + '/felfrekvens12mnd.csv' )
DF.tail(4) 
DF.iloc[2765:2769,:]


# In[152]:


#vill inte rita upp alla som inte är klassade, tar bort rader med felfall=okänd
DF.drop( DF[ DF['felfall'] == 'okänd' ].index , inplace=True)
DF=DF.dropna()
#DF=DF.drop_duplicates(subset=['felfall','veckonummer'])
DF.to_csv('C:/junk' + '/felfrekvens12mnd.csv' )

DF.tail(2) 


# In[153]:


import pandas as pd
import numpy as np
df=pd.read_csv('C:/junk' + '/felfrekvens12mnd.csv' )

df.to_csv('C:/junk' + '/felfrekvens12mnd_week.csv' )
df.dtypes
df.head(2)


# In[154]:


import altair as alt 
from vega_datasets import data
data=pd.read_csv('C:/junk' + '/felfrekvens12mnd.csv', encoding='utf-8' )

color = alt.Color('felfall:N')

# We create two selections:
# - a brush that is active on the top panel
# - a multi-click that is active on the bottom panel
brush = alt.selection_interval(encodings=['x'])
click = alt.selection_multi(encodings=['color'])

# Top panel is scatter plot of temperature vs time
points = alt.Chart().mark_line().encode(
    alt.X('week', axis=alt.Axis(title='veckonummer ')),
    alt.Y('Antal:Q',
        axis=alt.Axis(title='remarks/levererad instrument'),
    ),
    color=alt.condition(brush, color, alt.value('lightgray')),
    tooltip=['felfall', 'felfrekvens','week','Antal'],
).properties(
    width=600,
    height=200
).add_selection(
    brush
).transform_filter(
    click
)

# Bottom panel is a bar chart of species
bars = alt.Chart().mark_bar().encode(
    alt.Y('sum(Antal):Q', scale=alt.Scale(type='linear')),
     
    alt.X('felfall:N', sort=alt.EncodingSortField(field='Antal', 
             op='sum', order='descending')),
    color=alt.condition(click, color, alt.value('lightgray')),
    tooltip=['felfall','sum(Antal)','average(felfrekvens)'],
).transform_filter(
    brush
).properties(
    width=600,height=100,
).add_selection(
    click
)

legend = alt.Chart().mark_rect().encode(
    y=alt.Y('felfall:O', axis=alt.Axis(title='Välj felorsak')),
    color=alt.condition(click, 'felfall:O', 
                        alt.value('lightgray'), legend=None),
    size=alt.value(250)
).properties(
    selection=click
)    


chart=alt.vconcat(points|legend, alt.hconcat (bars),
    data=data,
    title="Felfrekvens per vecka: 2018-2019"
)
#.save('BinnedRemarksWeek1949.html', webdriver='firefox')

chart


# In[156]:


import altair as alt 
from vega_datasets import data
data=pd.read_csv('C:/junk' + '/felfrekvens12mnd.csv', encoding='utf-8' )

#data['felfrekvens']=0.3


color = alt.Color('detaljerad:N')

# We create two selections:
# - a brush that is active on the top panel
# - a multi-click that is active on the bottom panel
brush = alt.selection_interval(encodings=['x'])
click = alt.selection_multi(encodings=['color'])

# Top panel is scatter plot of temperature vs time
points = alt.Chart().mark_line().encode(
    alt.X('week', axis=alt.Axis(title='veckonummer ')),
    alt.Y('Antal:Q',
        axis=alt.Axis(title='remarks/levererad instrument'),
    ),
    color=alt.condition(brush, color, alt.value('lightgray')),
    tooltip=['detaljerad', 'felfrekvens','week','Antal'],
).properties(
    width=600,
    height=200
).add_selection(
    brush
).transform_filter(
    click
)

# Bottom panel is a bar chart of species
bars = alt.Chart().mark_bar().encode(
    alt.Y('sum(Antal):Q', scale=alt.Scale(type='linear')),
    
#    alt.X('felfall:N', sort=alt.EncodingSortField(field='felfall.value_counts(sort=True)', 
#            order='descending', op='count')),
    
    alt.X('detaljerad:N', sort=alt.EncodingSortField(field='Antal', 
             op='sum', order='descending')),
    color=alt.condition(click, color, alt.value('lightgray')),
    tooltip=['detaljerad','sum(Antal)','average(felfrekvens)'],
).transform_filter(
    brush
).properties(
    width=600,height=100,
).add_selection(
    click
)

legend = alt.Chart().mark_rect().encode(
    y=alt.Y('detaljerad:O', axis=alt.Axis(title='Välj felorsak')),
    color=alt.condition(click, 'detaljerad:O', 
                        alt.value('lightgray'), legend=None),
    size=alt.value(250)
).properties(
    selection=click
)    


chart=alt.vconcat(points, alt.hconcat (bars),
    data=data,
    title="Felfrekvens per vecka: 2018-2019"
)
#.save('BinnedRemarksWeek1949.html', webdriver='firefox')

chart


# In[ ]:


bars = alt.Chart(DF).mark_bar().encode(
    alt.Y('count(Antal)', scale=alt.Scale()),
    alt.X('Cause:O'),
    
).properties(
    width=600,
)
bars


# In[ ]:


import matplotlib.pyplot as plt
lean = pd.DataFrame(remarks.groupby(["Unit" ])["ErrorDate", "Test"].last())

lean.groupby([lean["ErrorDate"].dt.year, lean["ErrorDate"].dt.week]).count()
#lean.head(9)
plt.title('Deflection-relaterade fel')
plt.hist(lean['ErrorDate'], bins=65)#65 weeek
plt.show()


# In[61]:


import matplotlib.pyplot as plt

import pandas as pd
import matplotlib.pyplot as plt

s = pd.Series([1, 2, 3])
fig, ax = plt.subplots()
s.plot.bar()
fig.savefig('my_plot.png')
#fig.savefig(img, format='png')
#fig.savefig(img, src='my_plot.png')


# In[ ]:


#endast delsätt med 57m mål
import altair as alt



#plotta avståndsfel och korrigering

alt.Chart(df_merge.loc[df_merge['Pillar']=='Final Test Pillar 2',:]).mark_point().encode(
    x='Date',
    y=('W4W'),
    color='Pillar'
  )


# In[ ]:




#Steg 1 Det är denna jag jobbar med för Andrew, gruppvisa förbättringar
import pyodbc
import csv
import pandas as pd
import altair as alt

#stationMOFA='Sally_MOFALaser'
#testMOFA="'Calibrate MOFA'"
#deltestMOFA='OphirPower_1333W_26667Hz_W'
#OphirPower_1333W_26667Hz_W
#test="'Tx Transmission'"
#print(test)
#station='Optical Alignment 1'



db = pyodbc.connect('DRIVER={SQL Server};database=Produktion;server=sed-eu-db-prodrep;uid=analys;pwd=!al2005!')
cursor = db.cursor()



fromString_float = "TraceabilitySystem.dbo.ProductRemarks as fel join TraceabilitySystem.dbo.Symptoms as s on s.ID = fel.SymptomID join TraceabilitySystem.dbo.States as st on st.ID = fel.ReportStateID join TraceabilitySystem.dbo.Products as p on fel.ProductID = p.SuperProductID left join TraceabilitySystem.dbo.Products as pr on fel.productid = pr.id left join TraceabilitySystem.dbo.models as m on pr.modelid = m.id left join TraceabilitySystem.dbo.RemarkCauseSolutions as atg on fel.ID = atg.RemarkID left join TraceabilitySystem.dbo.ProductSerials as pold on atg.oldcomponent = pold.ProductID left join TraceabilitySystem.dbo.Products as prold on pold.productid = prold.id left join TraceabilitySystem.dbo.Solutions as sol on sol.ID = atg.SolutionID left join TraceabilitySystem.dbo.Causes as c on c.ID = atg.CauseID left join TraceabilitySystem.dbo.ProductRevisions as r on r.ProductID = pold.ProductID join TraceabilitySystem.dbo.ProductSerials as ps on    ps.ProductID  =p.SuperProductID join Produktion.dbo.PT_Main as main on ps.Serial = main.ProductName collate SQL_Latin1_General_CP1_CI_AS join Produktion.dbo.PT_PartTest as pt on main.ID = pt.PT_Main_Id join Produktion.dbo.PT_Value as v on pt.ID = v.PT_PartTest_Id join Produktion.dbo.PT_Value_Name as vn on vn.ID = v.PT_Value_Name_ID join Produktion.dbo.PT_Value_float as vf on vf.PT_Value_name_Id = v.ID join Produktion.dbo.PT_Main as MOFA on pold.Serial = MOFA.ProductName collate SQL_Latin1_General_CP1_CI_AS join Produktion.dbo.PT_PartTest as ptMOFA on MOFA.ID = ptMOFA.PT_Main_Id join Produktion.dbo.PT_Value as vMOFA on ptMOFA.ID = vMOFA.PT_PartTest_Id join Produktion.dbo.PT_Value_Name as vnMOFA on vnMOFA.ID = vMOFA.PT_Value_Name_ID join Produktion.dbo.PT_Value_float as vfMOFA on vfMOFA.PT_Value_name_Id = vMOFA.ID "

#Ta bort så mycket som möjligt!
#join TraceabilitySystem.dbo.States as st2 on st2.ID = pr.currentStateID \
#join aspnetdb.dbo.aspnet_Users as u2 on u2.UserId = fel.ReporterID \
#left join TraceabilitySystem.dbo.modelgroups as mg on mg.id = m.modelgroupid \
#left join TraceabilitySystem.dbo.models as m2 on prold.modelid = m2.id \
#left join aspnetdb.dbo.aspnet_Users as u1 on u1.UserId = atg.TroubleshooterID \

#SQL input parametrar
fieldsToSelect = """ps.Serial as [Instrument], pold.serial as [MOFA], st.name as [st], fel.Timestamp [ErrorDate], s.symptom [Symptom], fel.Comment [SymptomComment], 
st.name as [ErrorState], sol.Solution, atg.Solutioncomment, atg.timestamp [SolutionDate], main.prj as Station, pt.name as Test, main.TestStand [Pillar]
, 
 atg.causecomment [CauseComment], c.cause [Cause], m.name [Model], pr.createdtimestamp [UnitCreated]],
"""

förutsätning= "m.name ='SX10' and fel.Timestamp > '2018-01-01' and fel.comment like '%EDM SpotDirt%'     and main.date_time =fel.Timestamp  order by fel.Timestamp ASC"
#and pt.name like "+test+
fråga=" fel.Timestamp [ErrorDate], ps.serial as [Instrument],  pold.serial as [MOFA], fel.Comment [SymptomComment], atg.Solutioncomment as [Solution], atg.causecomment as [CauseComment], MOFA.date_time as [MOFAdate],    main.TestStand as [Pillar], fel.Comment [SymptomComment], atg.Solutioncomment as [Solution], atg.causecomment as [CauseComment]"
#####################################################,  
sqlQuery = "select distinct" +fråga+ " from " +fromString_float+ " where " +förutsätning 
#######################################################
cursor1 = db.execute(sqlQuery)
rows = cursor1.fetchall()
#print(rows)
db.commit()
db.close()
remarks = pd.DataFrame(columns=["ErrorDate","Instrument", "MOFA", "MOFAdate","MOFA-P","Pillar","Rack", "Fel","Symptom","Lösning"])
#remarks = pd.DataFrame(columns=["ErrorDate","Instrument", "MOFA", "MOFAdate","MOFA-P","Fel","Symptom","Lösning","Pillar","Rack"]) # ,"Measure",
remarks["ErrorDate"] = [i[0] for i in rows]
remarks["Instrument"]= [i[1] for i in rows]
remarks["MOFA"]      = [i[2] for i in rows]
remarks['MOFAdate']  = [i[6] for i in rows]
remarks["MOFA-P"]    = [i[7] for i in rows]
remarks["Pillar"]    = [i[8] for i in rows]
remarks['Rack']      = [i[9] for i in rows] 
#remarks["MOFAdeltest"]       = [i[5] for i in rows]
remarks["Fel"]  = [i[4] for i in rows]
remarks['MOFA-P']=remarks['MOFA-P']*1000
remarks["Symptom"]   = [i[5] for i in rows]
remarks["Lösning"]   = [i[3] for i in rows]


remarks=remarks.reset_index(drop=True)
remarks.head()
size = remarks.size
shape = remarks.shape
Antal=shape[0]
print('Antal',size, Antal)

remarks


# 

# In[ ]:





import pyodbc
import csv
import pandas as pd


db = pyodbc.connect('DRIVER={SQL Server};database=T4D;server=p-sally-se-de32  ;uid=toril.myrtveit@trimble.se;pwd=xxxx')
cursor = db.cursor()

############## Edit these filters to match what you want.##############

############################






#här är gramatiken till SQL
fromString_float = "Produktion.dbo.PT_Main as m join Produktion.dbo.PT_PartTest as pt on m.ID = pt.PT_Main_Id join Produktion.dbo.PT_Value as v on pt.ID = v.PT_PartTest_Id join Produktion.dbo.PT_Value_Name as vn on vn.ID = v.PT_Value_Name_ID join Produktion.dbo.PT_Value_float as vf on vf.PT_Value_name_Id = v.ID"
fromString_bool = "Produktion.dbo.PT_Main as m join Produktion.dbo.PT_PartTest as pt on m.ID = pt.PT_Main_Id join Produktion.dbo.PT_Value as v on pt.ID = v.PT_PartTest_Id join Produktion.dbo.PT_Value_Name as vn on vn.ID = v.PT_Value_Name_ID join Produktion.dbo.PT_Value_bool as vf on vf.PT_Value_name_Id = v.ID"
fromString_string = "Produktion.dbo.PT_Main as m join Produktion.dbo.PT_PartTest as pt on m.ID = pt.PT_Main_Id join Produktion.dbo.PT_Value as v on pt.ID = v.PT_PartTest_Id join Produktion.dbo.PT_Value_Name as vn on vn.ID = v.PT_Value_Name_ID join Produktion.dbo.PT_Value_string as vf on vf.PT_Value_name_Id = v.ID"

#SQL input parametrar
#fieldsToSelect = "m.prj as Station, pt.name as Test, vn.name as Tolerance, m.productname as ID, m.date_time as Date, vf.data as Value, m.TestStand as Pillar"
fieldsToSelect = "m.prj as m, m.date_time as Date"


#fromString2 = "Produktion.dbo.PT_Main as m2 join Produktion.dbo.PT_PartTest as pt2 on m2.ID = pt2.PT_Main_Id join Produktion.dbo.PT_Value as v2 on pt2.ID = v2.PT_PartTest_Id join Produktion.dbo.PT_Value_Name as vn2 on vn2.ID = v2.PT_Value_Name_ID"
#onlyLatestByDateFilter = "m.Date_Time = (select max(m2.Date_Time) from " + fromString2 + " where m.prj = m2.prj and m.productname = m2.productname and pt.name = pt2.name and vn.name = vn2.name) "
#onlyFirstByDateFilter = "m.Date_Time = (select min(m2.Date_Time) from " + fromString2 + " where m.prj = m2.prj and m.productname = m2.productname and pt.name = pt2.name and vn.name = vn2.name) "

#filter = stationFilter+" and "+testFilter+" and "+toleranceFilter+" and vn.name like '%%' and " + instrumentFilter + " and " + dateFilter +" and " +  pillarFilter# + " and " + onlyFirstByDateFilter

sqlQuery = "select distinct " + fieldsToSelect + " from " + fromString_float #+ " where "+filter
#getMeasurementsQueryStart = timer()
cursor1 = db.execute(sqlQuery)




rows = cursor1.fetchall()
for row in rows:
   print (row)
#    print(str(row[0])+"\t" + str(row[1]) +"\t" + str(row[5]) +"\t" + str(row[3]) + "\t"+ row.Date.strftime("%Y-%m-%d %H:%M:%S") + "\t" + row.Pillar)
    


# In[ ]:


#date is object, convert#date is object, convert


# In[ ]:


import pandas as pd
from tracescrape import deviceType
#data=pd.read_csv("alltrace.csv",index_col=0,parse_dates=[2])
data=pd.read_csv("alltrace.csv",index_col=0,parse_dates=[2])
data=data[data.timestamp>pd.to_datetime("2018-06-01")]
data=data[data.timestamp<pd.to_datetime("2018-06-30")].drop_duplicates(subset=["timestamp","traceData"])
aggdata=pd.DataFrame(data.groupby(["deviceType","traceData"])["traceData"].count())
aggdata.columns= ["traceDataCount"]
aggdata=aggdata.sort_values(by="traceDataCount",ascending=False).reset_index()
aggdata.deviceType=[deviceType[device] for device in aggdata.deviceType]

deviceData=pd.DataFrame()
for device in aggdata.deviceType.unique():
    deviceData=deviceData.append(aggdata[aggdata.deviceType==device].iloc[:20,0:4].reset_index())
deviceData

