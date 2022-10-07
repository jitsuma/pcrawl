#!/usr/bin/env python
# coding: utf-8

# In[49]:


import pandas as pd
import numpy as np
df = pd.read_csv('contacts.csv',encoding='latin-1', delimiter=';')

df = df.dropna(subset=['city', 'state','zipcode'], how='all')
df['zipcode'] = df['zipcode'].fillna('').astype(str)
df['zipcode'] = df['zipcode'].astype(str).str.replace('\.0', '')
df['city'] = df['city'].replace(' ', '-',regex=True).str.lower()
df['state'] = df['state'].str.lower()
df = df.reset_index()
df = df.drop(columns={'index'})

df


# In[50]:


df['budget_categories'] = 0
df['low'] = 0
df['high'] = 0
for i in range (0,len(df)):
    if df['budget'][i] == '$399 Or Less':
        df['budget_categories'][i] = 1
        df['low'][i] = 399 * 0.8
        df['high'][i] = 399 * 1.2 
    elif df['budget'][i] == '$400 - $699':
        df['budget_categories'][i] = 2
        df['low'][i] = 400 * 0.8
        df['high'][i] = 699 * 1.2
    elif df['budget'][i] == '$700 - $999':
        df['budget_categories'][i] = 3
        df['low'][i] = 700 * 0.8
        df['high'][i] = 999 * 1.2
    elif df['budget'][i] == '$1000+':
        df['budget_categories'][i] = 4
        df['low'][i] = 1000 
        df['high'][i] = 1000 * 1.2
    else:
        df['budget_categories'][i] = 5
        df['low'][i] = 400
        df['high'][i] = 1000 
df


# In[51]:


for i in range (0,len(df)):
    xx = len(df['zipcode'][i])
    print(xx)
    if(xx == 4):
        print('aaa')
        df['zipcode'][i] = '0'+ df['zipcode'][i]


# In[52]:


###### READ FILES FROM DATABASE ######
from sqlalchemy import create_engine
import pymysql
import pandas as pd

sqlEngine       = create_engine('mysql+pymysql://nearby_admin:2Zer8_2t4_SAD32f32dDGF3452@nearby2.c9w6hgaskmtt.us-west-1.rds.amazonaws.com/property_outreach', pool_recycle=3600)
dbConnection    = sqlEngine.connect()
frame           = pd.read_sql("select * from geo", dbConnection);

pd.set_option('display.expand_frame_repr', False)
print(frame)
df_geo = frame
dbConnection.close()
df_geo


# In[105]:


df_merge_geo = pd.merge(df,df_geo[['parent_id','name','lat','lng']], left_on='zipcode', right_on='name', how='left')
df_merge_geo['parent_id'] = df_merge_geo['parent_id'].astype(str).str.replace('\.0', '')
# df_merge_geo[df_merge_geo['zipcode'].notna()]
# df_merge_geo = df_merge_geo.reset_index()
# # df_merge_geo = df_merge_geo.drop(columns={'level_0'})
# df_merge_geo['index'] = df_merge_geo.index+1
df_merge_geo


# In[106]:


df_geo['id']= df_geo['id'].apply(str)
df_merge2 = pd.merge(df_merge_geo,df_geo[['id','parent_id','name']], left_on='parent_id', right_on='id', how='left')
df_merge2['parent_id_y'] = df_merge2['parent_id_y'].astype(str).str.replace('\.0', '')
df_merge2['name_y'] = df_merge2['name_y'].replace(' ', '-',regex=True).str.lower()

df_merge3 = pd.merge(df_merge2, df_geo[['id','name']], left_on='parent_id_y', right_on='id', how='left')
df_merge3['name'] = df_merge3['name'].replace(' ', '-',regex=True).str.lower()

df_state = pd.read_excel('state_list.xlsx')
df_state['state_name'] = df_state['state_name'].replace(' ', '-',regex=True).str.lower()
df_merge4 = pd.merge(df_merge3,df_state, left_on='name', right_on='state_name', how='left')

df_merge4 = df_merge4.drop(columns={'id','name','name_x'})
df_merge4 = df_merge4.rename(columns={'id_y':'city_id_geo','parent_id_x':'parent_id','name_y':'city_name','id_x':'id'})
df_merge4

for i in range (0, len(df_merge4)):
    if (df_merge4['parent_id_y'][i]== 'nan'):
        print(i)
        df_merge4['city_name'][i] = df_merge4['city'][i]
        df_merge4['state_code'][i] = df_merge4['state'][i]


# In[107]:


df_merge5 = df_merge4
df_merge5['city']=df_merge5['city_name']
df_merge5['state']=df_merge5['state_code'].str.lower()
df_merge5=df_merge5[df_merge5['city_name'].notna()]
df_merge5 = df_merge5.reset_index()
df_merge5 = df_merge5.drop(columns={'index'})
df_merge5


# In[109]:


df_geo['id']= df_geo['id'].apply('int64')
df_geo.dtypes


# In[110]:


aa = []
parent_url = []
parent_first_name = []
parent_last_name = []
parent_zip = []
parent_applicant_id = []
parent_email = []
parent_phone = []
parent_city = []
parent_state = []

for i in range(0,len(df_merge5)):
    print (i)
    a = df_merge5['parent_id'][i]
    city = df_merge5['city'][i]
    state = df_merge5['state'][i]
    zipp = df_merge5['zipcode'][i]
    low = df_merge5['low'][i]
    high = df_merge5['high'][i]
    
    first_name = df_merge5['first_name'][i]
    last_name = df_merge5['last_name'][i]
    applicant_id = df_merge5['id'][i]
    email = df_merge5['email'][i]
    phone = df_merge5['phone'][i]
    
    cur_lat = df_merge5['lat'][i]
    cur_lng = df_merge5['lng'][i]
    radius = 1.50
    lat_p = cur_lat+radius
    lat_n = cur_lat-radius
    lng_p = cur_lng+radius
    lng_n = cur_lng-radius
    print('zipppp: '+zipp)
    limit = 15
    limit2 = 2
    limit3 = 15

    if (zipp == '0'):        
        try:
            cur_lat = df_geo[df_geo['name'].replace(' ', '-',regex=True).str.lower() == city]['lat'].values[0]
            cur_lng = df_geo[df_geo['name'].replace(' ', '-',regex=True).str.lower() == city]['lng'].values[0]
        except:
            print('city lat-lng not found')
        print(cur_lat)
        print(cur_lng)
        radius = 1.50
        lat_p = cur_lat+radius
        lat_n = cur_lat-radius
        lng_p = cur_lng+radius
        lng_n = cur_lng-radius
        
        print('asd')
        aa.append("'" + "https://www.apartments.com/" + str(city) + "-" + str(state) + "/" + str(low) + "-to-" + str(high) + "',\n")
        parent_url.append("https://www.apartments.com/" + str(city) + "-" + str(state) + "/" + str(low) + "-to-" + str(high) + "/")
        parent_first_name.append(first_name)
        parent_last_name.append(last_name)
        parent_zip.append(zipp)
        parent_applicant_id.append(applicant_id)
        parent_email.append(email)
        parent_phone.append(phone)
        parent_city.append(city)
        parent_state.append(state)
        
        
        for i in (df_geo[(df_geo['lat'] > lat_n) & (df_geo['lat']<lat_p) & (df_geo['lng']> lng_n) & (df_geo['lng']< lng_p)]['name'].values):
            if ((i.isnumeric()) & (limit3 > 0)):
                print ('$$$$$$$$$$$$$$$$$$$$')
                print ('Zipcode: '+i)
                city_id = df_geo[df_geo['name'] == i]['parent_id'].values
                print('city_id: '+str(city_id[0]))
                city_name = df_geo[df_geo['id'] == city_id[0]]['name'].values
                print('city name:'+str(city_name[0]))
                state_id = df_geo[df_geo['id'] == city_id[0]]['parent_id'].values
                state_name = df_geo[df_geo['id']==state_id[0]]['short_name'].values[0]
                print ('State ID: ' + str(state_id[0]))
                print('State Name:'+str(state_name))
                print ('------- ####### -------') 
                city_name = city_name[0].replace(' ', '-')
                aa.append("'" + "https://www.apartments.com/" + city_name.lower() + "-" + state_name.lower() + "-" + i + "/" + str(low) + "-to-" + str(high) + "',\n")
                parent_url.append("https://www.apartments.com/" + str(city_name.lower()) + "-" + str(state_name.lower()) + "-" + i + "/" + str(low) + "-to-" + str(high) + "/")
                parent_first_name.append(first_name)
                parent_last_name.append(last_name)
                parent_zip.append(zipp)
                parent_applicant_id.append(applicant_id)
                parent_email.append(email)
                parent_phone.append(phone)
                parent_city.append(city)
                parent_state.append(state)
                limit3-=1
                print('this is i '+ i)
        
        
    if (a == 'nan'):
        aa.append("'" + "https://www.apartments.com/" + str(city) + "-" + str(state) + "/" + str(low) + "-to-" + str(high) + "',\n")
        parent_url.append("https://www.apartments.com/" + str(city) + "-" + str(state) + "/" + str(low) + "-to-" + str(high) + "/")
        parent_first_name.append(first_name)
        parent_last_name.append(last_name)
        parent_zip.append(zipp)
        parent_applicant_id.append(applicant_id)
        parent_email.append(email)
        parent_phone.append(phone)
        parent_city.append(city)
        parent_state.append(state)
        
        
    elif ((df_geo[df_geo['parent_id'] == int(a)]['name'].count()) < 3):
        print('This is only 1 Zipcode')
        for i in df_geo[df_geo['parent_id'] == int(a)]['name'].values:
            print(i)
            aa.append("'" + "https://www.apartments.com/" + str(city) + "-" + str(state) + "-" + i + "/" + str(low) + "-to-" + str(high) + "',\n")
            parent_url.append("https://www.apartments.com/" + str(city) + "-" + str(state) + "-" + i + "/" + str(low) + "-to-" + str(high) + "/")
            parent_first_name.append(first_name)
            parent_last_name.append(last_name)
            parent_zip.append(zipp)
            parent_applicant_id.append(applicant_id)
            parent_email.append(email)
            parent_phone.append(phone)
            parent_city.append(city)
            parent_state.append(state)
        for i in (df_geo[(df_geo['lat'] > lat_n) & (df_geo['lat']<lat_p) & (df_geo['lng']> lng_n) & (df_geo['lng']< lng_p)]['name'].values):
            if ((i.isnumeric()) & (limit > 0)):
                print ('There is less than 3 zipcode, so we expand')
                print ('Zipcode: '+i)
                city_id = df_geo[df_geo['name'] == i]['parent_id'].values
                print('city_id: '+str(city_id[0]))
                city_name = df_geo[df_geo['id'] == city_id[0]]['name'].values
                print('city name:'+str(city_name[0]))
                state_id = df_geo[df_geo['id'] == city_id[0]]['parent_id'].values
                state_name = df_geo[df_geo['id']==state_id[0]]['short_name'].values[0]
                print ('State ID: ' + str(state_id[0]))
                print('State Name:'+str(state_name))
                print ('------- ####### -------') 
                city_name = city_name[0].replace(' ', '-')
                aa.append("'" + "https://www.apartments.com/" + city_name.lower() + "-" + state_name.lower() + "-" + i + "/" + str(low) + "-to-" + str(high) + "',\n")
                parent_url.append("https://www.apartments.com/" + str(city_name.lower()) + "-" + str(state_name.lower()) + "-" + i + "/" + str(low) + "-to-" + str(high) + "/")
                parent_first_name.append(first_name)
                parent_last_name.append(last_name)
                parent_zip.append(zipp)
                parent_applicant_id.append(applicant_id)
                parent_email.append(email)
                parent_phone.append(phone)
                parent_city.append(city)
                parent_state.append(state)
                limit-=1  

    else:
        aa.append("'" + "https://www.apartments.com/" + str(city) + "-" + str(state) + "-" + str(zipp) + "/" + str(low) + "-to-" + str(high) + "',\n")
        parent_url.append("https://www.apartments.com/" + str(city) + "-" + str(state) + "-" + str(zipp) + "/" + str(low) + "-to-" + str(high) + "/")
        parent_first_name.append(first_name)
        parent_last_name.append(last_name)
        parent_zip.append(zipp)
        parent_applicant_id.append(applicant_id)
        parent_email.append(email)
        parent_phone.append(phone)
        parent_city.append(city)
        parent_state.append(state)
        for i in df_geo[df_geo['parent_id'] == int(a)]['name'].values:
            if (limit2 > 0):
                print(i)
                aa.append("'" + "https://www.apartments.com/" + str(city) + "-" + str(state) + "-" + i + "/" + str(low) + "-to-" + str(high) + "',\n")
                parent_url.append("https://www.apartments.com/" + str(city) + "-" + str(state) + "-" + i + "/" + str(low) + "-to-" + str(high) + "/")
                parent_first_name.append(first_name)
                parent_last_name.append(last_name)
                parent_zip.append(zipp)
                parent_applicant_id.append(applicant_id)
                parent_email.append(email)
                parent_phone.append(phone)
                parent_city.append(city)
                parent_state.append(state)
                print('---- END -----')
                limit2-=1

aa


# In[111]:


for i in range(0,len(df_merge5)):
    if (df_merge5['budget_categories'][i] != 5):
        
        if (df_merge5['budget_categories'][i] == 1):
            low = 320
            high = 838
        elif (df_merge5['budget_categories'][i] == 2):
            low = 560
            high = 1198
        elif (df_merge5['budget_categories'][i] == 3):
            low = 1000
            high = 1500
        elif (df_merge5['budget_categories'][i] == 4):
            low = 1200
            high = 1800
            
        a = df_merge5['parent_id'][i]
        city = df_merge5['city'][i]
        state = df_merge5['state'][i]
        zipp = df_merge5['zipcode'][i]
#         low = df_merge_geo['low'][i]
#         high = df_merge_geo['high'][i]

        first_name = df_merge5['first_name'][i]
        last_name = df_merge5['last_name'][i]
        applicant_id = df_merge5['id'][i]
        email = df_merge5['email'][i]
        phone = df_merge5['phone'][i]

        cur_lat = df_merge5['lat'][i]
        cur_lng = df_merge5['lng'][i]
        radius = 1.50
        lat_p = cur_lat+radius
        lat_n = cur_lat-radius
        lng_p = cur_lng+radius
        lng_n = cur_lng-radius
        print('zipppp: '+zipp)
        limit = 15
        limit2 = 2
        limit3 = 15

        if (zipp == '0'):        
            cur_lat = df_geo[df_geo['name'].replace(' ', '-',regex=True).str.lower() == city]['lat'].values[0]
            cur_lng = df_geo[df_geo['name'].replace(' ', '-',regex=True).str.lower() == city]['lng'].values[0]
            print(cur_lat)
            print(cur_lng)
            radius = 1.50
            lat_p = cur_lat+radius
            lat_n = cur_lat-radius
            lng_p = cur_lng+radius
            lng_n = cur_lng-radius

            print('asd')
            aa.append("'" + "https://www.apartments.com/" + str(city) + "-" + str(state) + "/" + str(low) + "-to-" + str(high) + "',\n")
            parent_url.append("https://www.apartments.com/" + str(city) + "-" + str(state) + "/" + str(low) + "-to-" + str(high) + "/")
            parent_first_name.append(first_name)
            parent_last_name.append(last_name)
            parent_zip.append(zipp)
            parent_applicant_id.append(applicant_id)
            parent_email.append(email)
            parent_phone.append(phone)
            parent_city.append(city)
            parent_state.append(state)


            for i in (df_geo[(df_geo['lat'] > lat_n) & (df_geo['lat']<lat_p) & (df_geo['lng']> lng_n) & (df_geo['lng']< lng_p)]['name'].values):
                if ((i.isnumeric()) & (limit3 > 0)):
                    print ('$$$$$$$$$$$$$$$$$$$$')
                    print ('Zipcode: '+i)
                    city_id = df_geo[df_geo['name'] == i]['parent_id'].values
                    print('city_id: '+str(city_id[0]))
                    city_name = df_geo[df_geo['id'] == city_id[0]]['name'].values
                    print('city name:'+str(city_name[0]))
                    state_id = df_geo[df_geo['id'] == city_id[0]]['parent_id'].values
                    state_name = df_geo[df_geo['id']==state_id[0]]['short_name'].values[0]
                    print ('State ID: ' + str(state_id[0]))
                    print('State Name:'+str(state_name))
                    print ('------- ####### -------') 
                    city_name = city_name[0].replace(' ', '-')
                    aa.append("'" + "https://www.apartments.com/" + city_name.lower() + "-" + state_name.lower() + "-" + i + "/" + str(low) + "-to-" + str(high) + "',\n")
                    parent_url.append("https://www.apartments.com/" + str(city_name.lower()) + "-" + str(state_name.lower()) + "-" + i + "/" + str(low) + "-to-" + str(high) + "/")
                    parent_first_name.append(first_name)
                    parent_last_name.append(last_name)
                    parent_zip.append(zipp)
                    parent_applicant_id.append(applicant_id)
                    parent_email.append(email)
                    parent_phone.append(phone)
                    parent_city.append(city)
                    parent_state.append(state)
                    limit3-=1
                    print('this is i '+ i)
        
        if (a == 'nan'):
            aa.append("'" + "https://www.apartments.com/" + str(city) + "-" + str(state) + "/" + str(low) + "-to-" + str(high) + "',\n")
            parent_url.append("https://www.apartments.com/" + str(city) + "-" + str(state) + "/" + str(low) + "-to-" + str(high) + "/")
            parent_first_name.append(first_name)
            parent_last_name.append(last_name)
            parent_zip.append(zipp)
            parent_applicant_id.append(applicant_id)
            parent_email.append(email)
            parent_phone.append(phone)
            parent_city.append(city)
            parent_state.append(state)


        elif ((df_geo[df_geo['parent_id'] == int(a)]['name'].count()) < 3):
            print('This is only 1 Zipcode')
            for i in df_geo[df_geo['parent_id'] == int(a)]['name'].values:
                print(i)
                aa.append("'" + "https://www.apartments.com/" + str(city) + "-" + str(state) + "-" + i + "/" + str(low) + "-to-" + str(high) + "',\n")
                parent_url.append("https://www.apartments.com/" + str(city) + "-" + str(state) + "-" + i + "/" + str(low) + "-to-" + str(high) + "/")
                parent_first_name.append(first_name)
                parent_last_name.append(last_name)
                parent_zip.append(zipp)
                parent_applicant_id.append(applicant_id)
                parent_email.append(email)
                parent_phone.append(phone)
                parent_city.append(city)
                parent_state.append(state)
            for i in (df_geo[(df_geo['lat'] > lat_n) & (df_geo['lat']<lat_p) & (df_geo['lng']> lng_n) & (df_geo['lng']< lng_p)]['name'].values):
                if ((i.isnumeric()) & (limit > 0)):
                    print ('There is less than 3 zipcode, so we expand')
                    print ('Zipcode: '+i)
                    city_id = df_geo[df_geo['name'] == i]['parent_id'].values
                    print('city_id: '+str(city_id[0]))
                    city_name = df_geo[df_geo['id'] == city_id[0]]['name'].values
                    print('city name:'+str(city_name[0]))
                    state_id = df_geo[df_geo['id'] == city_id[0]]['parent_id'].values
                    state_name = df_geo[df_geo['id']==state_id[0]]['short_name'].values[0]
                    print ('State ID: ' + str(state_id[0]))
                    print('State Name:'+str(state_name))
                    print ('------- ####### -------') 
                    city_name = city_name[0].replace(' ', '-')
                    aa.append("'" + "https://www.apartments.com/" + city_name.lower() + "-" + state_name.lower() + "-" + i + "/" + str(low) + "-to-" + str(high) + "',\n")
                    parent_url.append("https://www.apartments.com/" + str(city_name.lower()) + "-" + str(state_name.lower()) + "-" + i + "/" + str(low) + "-to-" + str(high) + "/")
                    parent_first_name.append(first_name)
                    parent_last_name.append(last_name)
                    parent_zip.append(zipp)
                    parent_applicant_id.append(applicant_id)
                    parent_email.append(email)
                    parent_phone.append(phone)
                    parent_city.append(city)
                    parent_state.append(state)
                    limit-=1  

        else:
            aa.append("'" + "https://www.apartments.com/" + str(city) + "-" + str(state) + "-" + str(zipp) + "/" + str(low) + "-to-" + str(high) + "',\n")
            parent_url.append("https://www.apartments.com/" + str(city) + "-" + str(state) + "-" + str(zipp) + "/" + str(low) + "-to-" + str(high) + "/")
            parent_first_name.append(first_name)
            parent_last_name.append(last_name)
            parent_zip.append(zipp)
            parent_applicant_id.append(applicant_id)
            parent_email.append(email)
            parent_phone.append(phone)
            parent_city.append(city)
            parent_state.append(state)
            for i in df_geo[df_geo['parent_id'] == int(a)]['name'].values:
                if (limit2 > 0):
                    print(i)
                    aa.append("'" + "https://www.apartments.com/" + str(city) + "-" + str(state) + "-" + i + "/" + str(low) + "-to-" + str(high) + "',\n")
                    parent_url.append("https://www.apartments.com/" + str(city) + "-" + str(state) + "-" + i + "/" + str(low) + "-to-" + str(high) + "/")
                    parent_first_name.append(first_name)
                    parent_last_name.append(last_name)
                    parent_zip.append(zipp)
                    parent_applicant_id.append(applicant_id)
                    parent_email.append(email)
                    parent_phone.append(phone)
                    parent_city.append(city)
                    parent_state.append(state)
                    print('---- END -----')
                    limit2-=1

aa


# In[112]:


print (len(parent_first_name))
print (len(parent_last_name))
print (len(parent_zip))
print (len(parent_applicant_id))
print (len(parent_email))
print (len(parent_phone))
print (len(parent_city))
print (len(parent_state))


# In[113]:


df_a = pd.DataFrame()
df_a['crawler_link'] = aa
df_a['page'] = parent_url
df_a['p_applicant_id'] = parent_applicant_id
df_a['p_first_name'] = parent_first_name
df_a['p_last_name'] = parent_last_name
df_a['p_email'] = parent_email
df_a['p_phone'] = parent_phone
df_a['p_city'] = parent_city
df_a['p_state'] = parent_state
df_a['p_zipcode'] = parent_zip
df_a


# In[114]:


df_a = df_a.groupby(['p_applicant_id']).head(12)
df_a = df_a.reset_index()
df_a['index'] = df_a.index+1
df_a


# In[115]:


with open("file11.py", "w") as output:
    output.write('link = [')
    for i in df_a['crawler_link']:
        output.write(i)
    output.write(']')


# In[116]:


import os
try:
    os.remove('/Users/Jimmy/Desktop/scraper2-new2/scraper2/scraper/scraper/spiders/pcrawl/apartments_filter_unique_zipcode.csv')
except:
    print('zzz')


# In[1]:


from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

process = CrawlerProcess(get_project_settings())

process.crawl('apartment_list')
process.start() # the script will block here until the crawling is finished# scrapy crawl apartment_list


# # Crawl Here

# In[1]:


import pandas as pd
df_items = pd.read_csv('/Users/Jimmy/Desktop/scraper2-new2/scraper2/scraper/scraper/spiders/pcrawl/apartments_filter_unique_zipcode.csv',encoding='utf-8')
df_items


# In[14]:


new = df_items["page"].str.split("/", n = 5, expand = True)
  
# making separate first name column from new data frame
df_items["budget2"]= new[4]
budget = df_items["budget2"].str.split("-", n = 3, expand = True)
df_items["low2"]= budget[0]
df_items["high2"] = budget[2]
# making separate last name column from new data frame
# data["Last Name"]= new[1]
  
# # Dropping old Name columns
# data.drop(columns =["Name"], inplace = True)

# df_items['page'].str.split('/')[0]
df_items


# In[15]:


df_merge_items = pd.merge(df_a,df_items, on='page', how='left')
df_merge_items = df_merge_items[df_merge_items['address'].notna()]
df_merge_items


# In[16]:


df_merge_items = df_merge_items.drop_duplicates(subset=['p_applicant_id','address'], keep='first')
df_merge_items = df_merge_items.groupby('p_applicant_id').head(5)
df_merge_items


# In[17]:


df_merge_items['full_address'] = np.NaN
df_merge_items['city'] = np.NaN
df_merge_items['state'] = np.NaN
df_merge_items['zipcode2'] = np.NaN
df_merge_items['full_address'] = df_merge_items['full_address'].astype(str)
df_merge_items['city'] = df_merge_items['city'].astype(str)
df_merge_items['state'] = df_merge_items['state'].astype(str)
df_merge_items['zipcode2'] = df_merge_items['zipcode2'].astype(str)
# df_merge_items['phone_number'] = df_merge_items['phone_number'].astype(str).str.replace('-', '')
df_merge_items['phone_number'] = df_merge_items['phone_number'].astype(str).str[4:]

df_merge_items = df_merge_items.reset_index()
df_merge_items = df_merge_items.drop(columns = {'index','level_0'})
df_merge_items


# In[18]:


for i in range (0, len(df_merge_items)):
    if (len(df_merge_items['address'][i].split(',')) == 2):
        df_merge_items['full_address'][i] = df_merge_items['name'][i].split(',')[0]
        df_merge_items['city'][i] = df_merge_items['address'][i].split(',')[0]
        df_merge_items['state'][i] = df_merge_items['address'][i].split(',')[1][1:3]
        df_merge_items['zipcode2'][i] = df_merge_items['address'][i].split(',')[1][-5:]
    elif (len(df_merge_items['address'][i].split(',')) == 3):
        df_merge_items['full_address'][i] = df_merge_items['address'][i].split(',')[0]
        df_merge_items['city'][i] = df_merge_items['address'][i].split(',')[1][1:]
        df_merge_items['state'][i] = df_merge_items['address'][i].split(',')[2][1:3]
        df_merge_items['zipcode2'][i] = df_merge_items['address'][i].split(',')[2][-5:]
    else:
        df_merge_items['full_address'][i] = df_merge_items['address'][i].split(',')[0]
        df_merge_items['city'][i] = df_merge_items['address'][i].split(',')[-2][1:]
        df_merge_items['state'][i] = df_merge_items['address'][i].split(',')[-1][1:3]
        df_merge_items['zipcode2'][i] = df_merge_items['address'][i].split(',')[-1][-5:]
    print(i)

df_merge_items


# In[20]:


df_merge_items.to_excel('try10.xlsx')


# In[ ]:





# In[21]:


df_merge_formatted = pd.DataFrame()
df_merge_formatted['applicant_id'] = df_merge_items['p_applicant_id']
df_merge_formatted['link_applicant_id'] = 'https://landlord.rentown.net/?auto=' + df_merge_items['p_applicant_id'].astype(str) + '&source=sw'
df_merge_formatted['first_name'] = df_merge_items['p_first_name']
df_merge_formatted['last_name'] = df_merge_items['p_last_name']
df_merge_formatted['customer_name'] = df_merge_items['p_first_name'] + ' ' + df_merge_items['p_last_name']
df_merge_formatted['customer_phone'] = df_merge_items['p_phone']
df_merge_formatted['customer_email'] = df_merge_items['p_email']
df_merge_formatted['property_name'] = df_merge_items['name']
df_merge_formatted['full_address'] = df_merge_items['address']
df_merge_formatted['address'] = np.nan
df_merge_formatted['city'] = df_merge_items['city']
df_merge_formatted['state'] = df_merge_items['state']
df_merge_formatted['zipcode'] = df_merge_items['zipcode2']
df_merge_formatted['pm_phone'] = df_merge_items['phone_number']
df_merge_formatted['URL'] = df_merge_items['link_url']
df_merge_formatted


# In[22]:


print(len(df_merge_formatted['full_address'][38].split(',')))

for i in range (0, len(df_merge_formatted)):   
    if (pd.isnull(df_merge_formatted['customer_name'][i])):
        if(pd.isnull(df_merge_formatted['first_name'][i])):
            df_merge_formatted['customer_name'][i] = df_merge_formatted['last_name'][i]
        else:
            df_merge_formatted['customer_name'][i] = df_merge_formatted['first_name'][i]
            
    if (len(df_merge_formatted['full_address'][i].split(',')) == 2):
        df_merge_formatted['address'][i] = df_merge_formatted['property_name'][i].split(',')[0]
    else:
        df_merge_formatted['address'][i] = df_merge_formatted['full_address'][i].split(',')[0]
    
df_merge_formatted


# In[ ]:





# In[23]:


df_merge_formatted


# In[24]:


leng = int(len(df_merge_formatted['pm_phone']) / 10)
mod = int(len(df_merge_formatted['pm_phone']) % 10)
print(leng)
print(mod)
import numpy as np
a = []
for i in range (3,13):
    for j in range (0,leng):
        a.append(i)
#print(a)
for i in range (0,mod):
    a.append(12)


# In[25]:


df_merge_formatted['user_id'] =  a
df_state = pd.read_excel('state_list.xlsx')
df_merge_state = pd.merge(df_merge_formatted,df_state, left_on='state', right_on='state_code', how='left')

cols = ['user_id','link_applicant_id','applicant_id','first_name','last_name','customer_name','customer_phone','customer_email','property_name','full_address','address','city','state','state_name','zipcode','pm_phone','URL']
df_merge_state = df_merge_state[cols]
# cols = ['agent','applicant_id','customer_name','customer_phone','customer_email','name','full_address','address','city','state','zipcode','pm_phone','URL']
# df_merge_formatted = df_merge_formatted[cols]
df_merge_state



df_merge_state['state_name'] = df_merge_state['state_name'].str.lower()
df_merge_state['city'] = df_merge_state['city'].str.lower()
df_geo_google = pd.read_excel('cities.xlsx')
df_geo_google['state_name'] = df_geo_google['state_name'].str.lower()
df_geo_google['city_name'] = df_geo_google['city_name'].str.lower()

df_merge_col2 = pd.merge(df_merge_state, df_geo_google[['city_id','city_name','state_id','state_name']], left_on=['city','state_name'], right_on=['city_name','state_name'], how='left')
df_merge_col2['city_id'] = df_merge_col2['city_id'].fillna(0).astype(int)
df_merge_col2['state_id'] = df_merge_col2['state_id'].fillna(0).astype(int)
df_merge_col2 = df_merge_col2.drop(columns={'city_name'})
df_merge_col2['id'] = df_merge_col2.index+1

cols = ['id','user_id','link_applicant_id','applicant_id','first_name','last_name','customer_name','customer_phone','customer_email','property_name','full_address','address','city','state','state_name','zipcode','pm_phone','URL','city_id','state_id']
df_merge_col2 = df_merge_col2[cols]
df_merge_col2


# In[26]:


df_merge_col2.to_excel('link_zipcode_filter_with_customer_unique.xlsx',index=False)


# In[27]:


###### READ FILES FROM DATABASE ######
from sqlalchemy import create_engine
import pymysql
import pandas as pd

 

sqlEngine       = create_engine('mysql+pymysql://nearby_admin:2Zer8_2t4_SAD32f32dDGF3452@nearby2.c9w6hgaskmtt.us-west-1.rds.amazonaws.com/property_outreach', pool_recycle=3600)
dbConnection    = sqlEngine.connect()
frame           = pd.read_sql("select * from listings", dbConnection);

 

pd.set_option('display.expand_frame_repr', False)
print(frame)
df_listings_db = frame
dbConnection.close()
df_listings_db


# In[28]:


import datetime
import dateutil.relativedelta
now = datetime.datetime.now()
print (now + dateutil.relativedelta.relativedelta(months=-1))
df_listings_db['together'] = df_listings_db['property_name'].str.lower() + ' | ' + df_listings_db['full_address'].str.lower()
df_merge_col2['together'] = df_merge_col2['property_name'].str.lower() + ' | ' + df_merge_col2['full_address'].str.lower()


# In[29]:


df_merge_listings = pd.merge(df_merge_col2,df_listings_db[['id','pm_id','date','together']], on='together', how='left')
# df_merge_listings = df_merge_listings[(df_merge_listings['date'] < (now + dateutil.relativedelta.relativedelta(months=-1))) | (pd.isna(df_merge_listings.id_y))]
# df_merge_listings = df_merge_listings[pd.isna(df_merge_listings.id_y)]
df_merge_listings = df_merge_listings.rename(columns={'id_x':'id', 'id_y':'listing_id'})
df_merge_listings = df_merge_listings.reset_index()
df_merge_listings['id'] = df_merge_listings.index + 1
df_merge_listings = df_merge_listings.drop(columns={'together','index'})
df_merge_listings['listing_id'] = df_merge_listings['listing_id'].astype(str).str.replace('\.0', '')
df_merge_listings['pm_id'] = df_merge_listings['pm_id'].astype(str).str.replace('\.0', '')
# df_merge_listings['listing_id'] = df_merge_listings['listing_id'].fillna(NaN).astype(int)

 


# for i in range (0, len(df_merge_listings)):
#     if (pd.notna(df_merge_listings.listing_id[i])):
#         df_merge_listings['listing_id'][i] = df_merge_listings['listing_id'][i].astype(str).replace('\.0', '')
#         print (i)
df_merge_listings


# In[30]:


###### READ FILES FROM DATABASE ######
from sqlalchemy import create_engine
import pymysql
import pandas as pd

 

sqlEngine       = create_engine('mysql+pymysql://nearby_admin:2Zer8_2t4_SAD32f32dDGF3452@nearby2.c9w6hgaskmtt.us-west-1.rds.amazonaws.com/property_outreach', pool_recycle=3600)
dbConnection    = sqlEngine.connect()
frame           = pd.read_sql("select * from agent_property_lists", dbConnection);

 

pd.set_option('display.expand_frame_repr', False)
print(frame)
df_remainings_db = frame
dbConnection.close()
df_remainings_db


# In[31]:


df_merge_listings = df_remainings_db.append(df_merge_listings, ignore_index=True)
df_merge_listings = df_merge_listings.drop_duplicates(subset=['applicant_id','full_address'], keep='first')


leng = int(len(df_merge_listings['pm_phone']) / 10)
mod = int(len(df_merge_listings['pm_phone']) % 10)
print(leng)
print(mod)
import numpy as np
a = []
for i in range (3,13):
    for j in range (0,leng):
        a.append(i)
#print(a)
for i in range (0,mod):
    a.append(12)
df_merge_listings['user_id'] =  a
df_merge_listings['link_applicant_id'] = 'https://landlord.rentown.net/?auto=' + df_merge_listings['applicant_id'].astype(str).str.replace('\.0', '') + '&lid=' + df_merge_listings['listing_id'].astype(str) + '&pmid=' + df_merge_listings['pm_id'].astype(str) + '&source=sw'
df_merge_listings = df_merge_listings.reset_index()
df_merge_listings = df_merge_listings.drop(columns={'id'})
df_merge_listings = df_merge_listings.rename(columns={'index':'id'})
df_merge_listings


# In[32]:


df_b = df_merge_listings.groupby(["applicant_id"])["applicant_id"].count().reset_index(name="count")
df_b = df_b.groupby(['count']).agg(['count'])
df_b.to_excel('count_applicants.xlsx')
df_c = df_remainings_db.groupby(["user_id"])["user_id"].count().reset_index(name="count")
df_c.to_excel('count_agent_remaining.xlsx')


# In[ ]:





# In[ ]:


###### INSERT DATA to SQL ######

# import the module
from sqlalchemy import create_engine

# create sqlalchemy engine
engine = create_engine("mysql+pymysql://nearby_admin:2Zer8_2t4_SAD32f32dDGF3452@nearby2.c9w6hgaskmtt.us-west-1.rds.amazonaws.com/property_outreach"
                       .format(user="nearby_admin",
                               pw="2Zer8_2t4_SAD32f32dDGF3452",
                               db="agent_property_lists"))
# Insert whole DataFrame into MySQL
df_merge_listings.to_sql('agent_property_lists', con = engine, if_exists = 'replace', chunksize = 1000,index=False)


# In[ ]:


df_merge_listings.to_excel('fix_file.xlsx',index=False)


# In[ ]:




