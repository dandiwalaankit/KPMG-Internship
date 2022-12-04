#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Import the required librariesabs
import pandas as pd
import numpy as np


# In[2]:


rootDF_Transactions=pd.read_csv('G:\My Drive\KPMG\Task 2\Transactions_Done.csv')
rootDF_NewCustList=pd.read_csv(r'G:\My Drive\KPMG\Task 2\NewCustList_Done.csv')
rootDF_CustomerDemographic=pd.read_csv('G:\My Drive\KPMG\Task 2\CustomerDemographic_Done.csv')
rootDF_CustomerAddress=pd.read_csv('G:\My Drive\KPMG\Task 2\CustomerAddress_Done.csv')


# In[3]:


rootDF_Transactions=pd.read_excel('G:\My Drive\KPMG\Task 3\KPMG_ImportPython.xlsx',"Transactions")
rootDF_NewCustList=pd.read_excel('G:\My Drive\KPMG\Task 3\KPMG_ImportPython.xlsx',"NewCustomerList")
rootDF_CustomerDemographic=pd.read_excel('G:\My Drive\KPMG\Task 3\KPMG_ImportPython.xlsx',"CustomerDemographic")
rootDF_CustomerAddress=pd.read_excel('G:\My Drive\KPMG\Task 3\KPMG_ImportPython.xlsx',"CustomerAddress")


# In[4]:


dft=rootDF_Transactions.copy()
dft.head()


# In[5]:


dfcd=rootDF_CustomerDemographic.copy()
dfcd.head()


# In[6]:


dfca=rootDF_CustomerAddress.copy()
dfca.head()


# In[7]:


df12=pd.merge(dfcd, dft, left_on='customer_id', right_on='customer_id')


# In[8]:


df12


# In[9]:


#Checked if tables are joined using inner joint

#dft['customer_id'].value_counts() #Length: 3494
#dfcd['customer_id'].value_counts() #Length: 3999
#df12['customer_id'].value_counts() #Length: 3492


# In[10]:


#Checkin columns

dft.columns


# In[11]:


dfcd.columns


# In[12]:


df12.columns


# In[13]:


#Checking shape
dft.shape


# In[14]:


dfcd.shape


# In[15]:


df12.shape


# In[16]:


df=pd.merge(df12, dfca, left_on='customer_id', right_on='customer_id')
df


# In[17]:


dfca.shape


# In[18]:


df.shape


# In[19]:


len(df.axes[1]) 


# In[20]:


#Checking all categorical data inconsistency
df.columns


# In[21]:


df_columns=['gender','job_industry_category','wealth_segment','deceased_indicator','owns_car','online_order',
                'order_status','brand','product_line','product_class','product_size','postcode','state', 'country',]


# In[22]:


for i in df_columns:
    print(df[i].value_counts())


# In[23]:


#'gender': Replace Female and Femal with F. Replace Male with M
df=df.replace({'gender':{'Female':'F','Male':'M','Femal':'F'}})
df['gender'].value_counts()


# In[24]:


#'state': Replace NSW with New South Wales; Replace VIC with Victoria; Replace QLD with Queensland
df=df.replace({'state':{'NSW':'New South Wales','QLD':'Queensland','VIC':'Victoria'}})
df['state'].value_counts()


# In[25]:


df.isnull().sum()


# In[26]:


#Since the hiint was given to remove null cells, the cells have been deleted. Moreover, after performing groupby the length of the data frame would be around 3500 which is not suffcient for training model that too with too many categorical varriables.
df=df.dropna()


# In[27]:


df.shape


# In[28]:


df.isnull().sum()


# In[29]:


#Add age
now = pd.Timestamp('now')


# In[30]:


df['DOB']=pd.to_datetime(df['DOB'], format='%m%d%y')
df['DOB'].head()


# In[31]:


df['DOB']=df['DOB'].where(df['DOB']<now,df['DOB']-np.timedelta64(100,'Y'))
df['age']=(now-df['DOB']).astype('<m8[Y]')
df['age'].head()


# In[32]:


df['age'].describe()


# In[33]:


#Since the data are to be imported to the Tableau, no need to make bins for age and tenure columns


# In[34]:


df.shape


# In[35]:


df.to_csv('CombineWithDuplicate.csv', index=False)


# In[36]:


#Now Aggregating duplicate data
df.columns


# In[ ]:





# In[37]:


##In order to decide whether the variable are to be encoded or summed up while aggregating or delete, Checking .value_counts
#df['transaction_id'].value_counts() #Length: 12970
#df['product_id'].value_counts() #Length:101
#df['transaction_date'].value_counts() #Length:364
#df['online_order'].value_counts() #Length:2
#df['order_status'].value_counts() #Length:2
#df['brand'].value_counts() #Length:6
#df['product_line'].value_counts() #Length:4
#df['product_class'].value_counts() #Length:3
#df['product_size'].value_counts() #Length:3
#df['product_first_sold_date'].value_counts() #Length:100

##Decision has been made as below:
##Following variable are deleted:
##transaction_id,
##product_id,
##transaction_date,
##product_first_sold_date

##Following variables need to be encoded
##online_order,
##order_status,
##brand,
##product_line
##product_class,
##product_size


# In[38]:


df.columns


# In[39]:


df_columns= ['customer_id','first_name', 'last_name', 'gender','past_3_years_bike_related_purchases',
            'DOB', 'job_title','job_industry_category', 'wealth_segment', 'deceased_indicator','owns_car',
            'tenure', 'online_order', 'order_status', 'brand','product_line', 'product_class', 'product_size',
            'list_price','standard_cost', 'address', 'postcode', 'state', 'country',
            'property_valuation', 'age']


# In[40]:


for i in df_columns:
    index_no = df.columns.get_loc(df_columns)
    print("Index of {} column in given dataframe is : {}".format(df_columns, index_no))


# In[42]:


df=df.drop(['transaction_id','product_id','transaction_date','product_first_sold_date','default'], axis=1)
df.shape


# In[43]:


df.columns


# In[44]:


##Following variables need to be encoded
#df['online_order'].value_counts() #Length:2 #Column_index=13
#df['order_status'].value_counts() #Length:2 #Column_index=14
#df['brand'].value_counts() #Length:6 #Column_index=15
#df['product_line'].value_counts() #Length:4 #Column_index=16
#df['product_class'].value_counts() #Length:3 #Column_index=17
#df['product_size'].value_counts() #Length:3 #Column_index=18


# In[45]:


dfonline_order=pd.get_dummies(df.iloc[:,12])
dfonline_order.shape
dfonline_order.head()


# In[46]:


dforder_status=pd.get_dummies(df.iloc[:,13])
dforder_status.shape
dforder_status.head()


# In[47]:


dfbrand=pd.get_dummies(df.iloc[:,14])
dfbrand.shape
dfbrand.head()


# In[48]:


dfproduct_line=pd.get_dummies(df.iloc[:,15])
dfproduct_line.shape
dfproduct_line.head()


# In[49]:


dfproduct_class=pd.get_dummies(df.iloc[:,16])
dfproduct_class.shape
dfproduct_class.head()


# In[50]:


dfproduct_size=pd.get_dummies(df.iloc[:,17])
dfproduct_size.shape
dfproduct_size.head()


# In[51]:


dfonline_order.rename(columns = {0.0:'Offline order', 1.0:'online order'}, inplace = True)
dforder_status.rename(columns = {'Approved':'Order approved', 'Cancelled':'Order Cancelled'}, inplace = True)
dfproduct_line.rename(columns = {'Mountain':'Product line:Mountain','Road':'Product line:Road',
                                 'Standard':'Product line:Standard','Touring':'Product line:Touring'}, inplace = True)
dfproduct_class.rename(columns = {'high':'Product Class:High', 'low':'Product Class:Low',
                                  'medium':'Product Class:Medium'}, inplace = True)
dfproduct_size.rename(columns = {'large':'Product size:Large', 'medium':'Product size:Medium',
                                 'small':'Product size:Small'}, inplace = True)


# In[52]:


dfonline_order.columns


# In[53]:


dforder_status.columns


# In[54]:


dfproduct_line.columns


# In[55]:


dfproduct_class.columns


# In[56]:


dfproduct_size.columns


# In[57]:


df.head()


# In[58]:


df=df.join(dfonline_order)
df.head()


# In[59]:


df=df.join(dforder_status)
df.head()


# In[60]:


df=df.join(dfbrand)
df.head()


# In[61]:


df=df.join(dfproduct_line)
df.head()


# In[62]:


df=df.join(dfproduct_class)
df.head()


# In[63]:


df=df.join(dfproduct_size)
df.head()


# In[64]:


##Following variables need to be encoded
#df['online_order'].value_counts() #Length:2 #Column_index=13
#df['order_status'].value_counts() #Length:2 #Column_index=14
#df['brand'].value_counts() #Length:6 #Column_index=15
#df['product_line'].value_counts() #Length:4 #Column_index=16
#df['product_class'].value_counts() #Length:3 #Column_index=17
#df['product_size'].value_counts() #Length:3 #Column_index=18


# In[65]:


df.shape


# In[66]:


df=df.drop(['online_order', 'order_status', 'brand','product_line', 'product_class', 'product_size'], axis=1)
df.shape


# In[67]:


df.head()


# In[68]:


df.columns


# In[69]:


#Here grouping by 'customer_id' is being done. Some of the categorical data are not being encoded since the data is to be imported to the tableau and no machne learning model is to be applied. Therefore some of the such data will be counted while aggrgating. eg. 'poduct_id'
df=df.groupby(['customer_id']).agg({'first_name':'first', 'last_name':'first', 'gender':'first',
       'past_3_years_bike_related_purchases':'sum', 'DOB':'first', 'job_title':'first',
       'job_industry_category':'first', 'wealth_segment':'first', 'deceased_indicator':'first',
       'owns_car':'first', 'tenure':'first','list_price':'sum', 'standard_cost':'sum', 'address':'first',
       'postcode':'first', 'state':'first', 'country':'first', 'property_valuation':'first', 'age':'first',
       'Offline order':'sum','online order':'sum', 'Order approved':'sum', 'Order Cancelled':'sum',
       'Giant Bicycles':'sum', 'Norco Bicycles':'sum', 'OHM Cycles':'sum', 'Solex':'sum',
       'Trek Bicycles':'sum', 'WeareA2B':'sum', 'Product line:Mountain':'sum',
       'Product line:Road':'sum', 'Product line:Standard':'sum', 'Product line:Touring':'sum',
       'Product Class:High':'sum', 'Product Class:Low':'sum', 'Product Class:Medium':'sum',
       'Product size:Large':'sum', 'Product size:Medium':'sum', 'Product size:Small':'sum'}).reset_index()


# In[70]:


df.head()


# In[71]:


dfAgg.shape


# In[72]:


df.to_csv('CombineWithAggregate.csv', index=False)

