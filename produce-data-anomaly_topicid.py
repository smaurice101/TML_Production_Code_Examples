# Developed by: OTICS Advanced Analytics Inc.
# Date: 2021-01-18 
# Toronto, Ontario Canada
# For help email: support@otics.ca 

#######################################################################################################################################

# This file will produce data to a Kafka cluster for Bank Fraud Detection.  Before using this code you MUST have:
# 1) Downloaded and installed MAADS-VIPER from: https://github.com/smaurice101/transactionalmachinelearning

# 2) You have VIPER listening for a connection on port IP: http://127.0.01 and PORT: 9000 (you can specify different IP and PORT
#    just change the  VIPERHOST="http://127.0.0.1" and VIPERPORT=9000)

# 3) You have created a KAfka cluster in Confluent Cloud (https://confluent.cloud/)

# 4) You have updated the VIPER.ENV file in the following fields:
# a) KAFKA_CONNECT_BOOTSTRAP_SERVERS=[Enter the bootstrap server - this is the Kafka broker(s) - separate multiple brokers by a comma]
# b) KAFKA_ROOT=kafka
# c) SSL_CLIENT_CERT_FILE=[Enter the full path to client.cer.pem]
# d) SSL_CLIENT_KEY_FILE=[Enter the full path to client.key.pem]
# e) SSL_SERVER_CERT_FILE=[Enter the full path to server.cer.pem]

# f) CLOUD_USERNAME=[Enter the Cloud Username- this is the KEY]
# g) CLOUD_PASSWORD=[Enter the Cloud Password - this is the secret]

# NOTE: IF YOU GET STUCK WATCH THE YOUTUBE VIDEO: https://www.youtube.com/watch?v=b1fuIeC7d-8
# Or email support@otics.ca
#########################################################################################################################################


# Produce Data to Kafka Cloud
import maadstml

# Uncomment IF using Jupyter notebook 
#import nest_asyncio

import json
import random
from datetime import datetime
from random import randint
from joblib import Parallel, delayed

#import asyncio
#import aiohttp
from multiprocessing import Process
# Uncomment IF using Jupyter notebook
#nest_asyncio.apply()


# Set Global Host/Port for VIPER - You may change this to fit your configuration
VIPERHOST="https://127.0.0.1"
#VIPERHOST="http://192.168.0.101"
VIPERPORT=8000

#############################################################################################################
#                                      STORE VIPER TOKEN
# Get the VIPERTOKEN from the file admin.tok - change folder location to admin.tok
# to your location of admin.tok
def getparams():
        
     with open("C:/viperdemo/admin.tok", "r") as f:
        VIPERTOKEN=f.read()
  
     return VIPERTOKEN

VIPERTOKEN=getparams()

def random_with_N_digits(n):
    range_start = 10**(n-1)
    range_end = (10**n)-1
    return randint(range_start, range_end)

# Simulate product prices
def getproductprice(product):
   ru=1.0
   print(product)
   if product in ["Eggs","Bread","Milk","Fruits", "Vegetables","Meat","Coffee","Tea"]:
       ru=random.uniform(1.50, 10.9)
   elif product in ["Wine","Whisky"]:
       ru=random.uniform(13.50, 200.9)  
   elif product in ["Gasoline","Salon"]:
       ru=random.uniform(5.50, 50.9)  
   elif product in ["Tshirt","Blouse","Shirt","Shoes","Suit","Dress"]:
       ru=random.uniform(10.50, 6000.9)  
   elif product in ["Restaurant"]:
       ru=random.uniform(10.50, 1200.9)  
   elif product in ["Rent","MortgagePayment"]:
       ru=random.uniform(600.1,7000.9)
   elif product in ["Movie"]:
       ru=random.uniform(10.1,100.1)   
   elif product in ["Luxury"]:
       ru=random.uniform(1000.1,10000.1)   

   return round(ru,2)


#############################################################################################################
#                                     CREATE BANK ACCOUNT TOPICS IN KAFKA

# Set personal data
def datasetup(maintopic,totalaccounts,totaltrans):
     companyname="OTICS Advanced Analytics"
     myname="Sebastian"
     myemail="Sebastian.Maurice"
     mylocation="Toronto"
     # Replication factor for Kafka redundancy
     replication=3
     # Number of partitions for joined topic
     numpartitions=1
     # Enable SSL/TLS communication with Kafka
     enabletls=1
     # If brokerhost is empty then this function will use the brokerhost address in your
     # VIPER.ENV in the field 'KAFKA_CONNECT_BOOTSTRAP_SERVERS'
     brokerhost=''
     # If this is -999 then this function uses the port address for Kafka in VIPER.ENV in the
     # field 'KAFKA_CONNECT_BOOTSTRAP_SERVERS'
     brokerport=-999
     # If you are using a reverse proxy to reach VIPER then you can put it here - otherwise if
     # empty then no reverse proxy is being used
     microserviceid=''

     ##########################################################################################
     #                                   FAST CREATION OF TOPIC IN KAFKA

     description="TML Book example anomaly prediction"


     result=maadstml.vipercreatetopic(VIPERTOKEN,VIPERHOST,VIPERPORT,maintopic,companyname,
                                    myname,myemail,mylocation,description,enabletls,
                                    brokerhost,brokerport,numpartitions,replication,
                                    microserviceid)
     try:
          y = json.loads(result,strict='False') 
     except Exception as e:
          y = json.loads(result)

     for p in y:  # Loop through the JSON
         producerid=p['ProducerId']

     return producerid

######################################################################################################################
#                                      CREATE DUMMY DATA     
def sendtransactiondata(maintopic,producerid,bankaccounts,transactions,j):

        location=["Toronto","NewYork","London","Seoul","NewDelhi","Tokyo","Beijing","Munich","Australia","Mexico","Nairobi","Norway","Moscow"]

        currency=["CAD","USD","GBP","KRW","INR","JPY","CNY","EUR","AUD","MXN","KSH","NOK","RUB"]

        counterparty=["Walmart","Costco","Amazon","Daiei","Tesco","Auchan","Tiffany","LouisVuitton","Jara","Eliseyevskiy","Bank","Landlord"]
 
        productpurchased=["Eggs","Bread","Milk","Wine","Whisky","Gasoline","Coffee","Tea","Tshirt","Blouse","Shirt","Shoes","Salon","Movie","Restaurant",
                       "Rent","MortgagePayment","Fruits", "Vegetables","Meat","Luxury","Dress","Suit"]

        fields=["transactiondatetime","currency","productpurchased","amountpaid","location","transactionid","counterparty"]
        subtopics="otics_tml_transactiondatetime,otics_tml_currency,otics_tml_productpurchased,otics_tml_amountpaid,\
otics_tml_location,otics_tml_transactionid,otics_tml_counterparty"
#       for j in range(transactions):
    
        ap=500
        idx=0
        b=bankaccounts

        inputbuf=""

        #transactiondatetime
        inputbuf=inputbuf+ datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] +","  

        #currency
        r=random.randint(0,len(currency)-1)    
        inputbuf=inputbuf+ currency[r] +","
        idx=r
            
        #productpurchased
        r=random.randint(0,len(productpurchased)-1)
        product=productpurchased[r]
        inputbuf=inputbuf+ product +","  

        #amountpaid
        ap=getproductprice(product)
        inputbuf=inputbuf+ str(ap) +","  

        #location
        inputbuf=inputbuf+ location[idx] +","  

        #transactionid
        tid=str(random_with_N_digits(30))  
        inputbuf=inputbuf+ str(tid) +","  

        #counterparty
        if product in ["Luxury","Dress","Suit","Dress","Shirt","Shoes"] and ap>500:
              stores=["Tiffany","LouisVuitton"]
              r=random.randint(0,len(stores)-1)      
              inputbuf=inputbuf+ stores[r] +","
        elif ap<500 and product not in ["Luxury","Rent","MortgagePayment"]:
              stores=["Walmart","Costco","Amazon","Daiei","Tesco","Auchan","Jara","Eliseyevskiy"]
              r=random.randint(0,len(stores)-1)      
              inputbuf=inputbuf+ stores[r] +","
        elif ap>500 and product in ["Rent","MortgagePayment"]:
              stores=["Bank","Landlord"]
              r=random.randint(0,len(stores)-1)      
              inputbuf=inputbuf+ stores[r] +","
        else:
              r=random.randint(0,len(counterparty)-1)      
              inputbuf=inputbuf+ counterparty[r] +","   
                       
        inputbuf=inputbuf[:-1]
        print("SubTopicbuf=",subtopics)
        print("Input=",inputbuf)
        print("Sending Data for Bank Account=" + str(j))

        delay=7000
        enabletls=1
        topicid=j
        try:
           result=maadstml.viperproducetotopic(VIPERTOKEN,VIPERHOST,VIPERPORT,maintopic,producerid,enabletls,delay,'','', '',0,inputbuf,
                                                   subtopics,topicid)
        except Exception as e:
            print(e)
             

# Change the number of numberofbankaccounts or transactions
cycles=1000
numberofbankaccounts=3
transactions=50

maintopic="otics-tmlbook-anomaly-mainstream"

#setup the data
producerid=datasetup(maintopic,numberofbankaccounts,transactions)

#k=1
#sendtransactiondata(maintopic,producerid,numberofbankaccounts,transactions,k)

for c in range(cycles):
   for k in range(numberofbankaccounts):
     element_run = Parallel(n_jobs=-1)(delayed(sendtransactiondata)(maintopic,producerid,numberofbankaccounts,transactions,k) for m in range(transactions))
  

