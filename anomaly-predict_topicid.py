# Developed by: OTICS Advanced Analytics Inc.
# Date: 2021-01-18 
# Toronto, Ontario Canada
# For help email: support@otics.ca 

#######################################################################################################################################

# This file will perform TML for Bank Fraud Detection.  Before using this code you MUST have:

# 1) Downloaded and installed MAADS-VIPER and MAADS-HPDE: from: https://github.com/smaurice101/transactionalmachinelearning

# 2) You have:
#    a) VIPER listening for a connection on port IP: http://127.0.01 and PORT: 8000 (you can specify different IP and PORT
#    just change the  VIPERHOST="http://127.0.0.1" and VIPERPORT=8000)

#    b) HPDE listening for a connection on port IP: http://127.0.01 and PORT: 8001 (you can specify different IP and PORT
#    just change the  hpdehost="http://127.0.0.1" and hpdeport=8001)                                                                                      
                                                                                      
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


# import Python Libraries
import maadstml
# Uncomment IF using jupyter notebook
#import nest_asyncio
import threading
import json
import time

from joblib import Parallel, delayed
import multiprocessing

from multiprocessing import Process

# Uncomment IF using jupyter notebook
#nest_asyncio.apply()

# Set Global variables for VIPER and HPDE - You can change IP and Port for your setup of 
# VIPER and HPDE
VIPERHOST="https://127.0.0.1"
VIPERPORT=8000
hpdehost="http://127.0.0.1"
hpdeport=8001

# Set Global variable for Viper confifuration file - change the folder path for your computer
viperconfigfile="C:/viperdemo/viper.env"

#############################################################################################################
#                                      STORE VIPER TOKEN
# Get the VIPERTOKEN from the file admin.tok - change folder location to admin.tok
# to your location of admin.tok
def getparams():
        
     with open("C:/viperdemo/admin.tok", "r") as f:
        VIPERTOKEN=f.read()
  
     return VIPERTOKEN

VIPERTOKEN=getparams()

def streamstocheckforanomalies():
     streams=["otics_tml_currency","otics_tml_productpurchased","otics_tml_amountpaid","otics_tml_location","otics_tml_counterparty"]
     
     topicnames=','.join(streams)     
     return topicnames


# Function inputs
# NOTE: We want choose a good peer-group for the anomaly training dataset to do so we MUST remove any
# non-normal values, because these are the values we are trying to detect. 
# allstreams =all streams to check for anomalies
# stringthreshnumber= risk threshold number for string values, to determine peer group - any value above this number is not in peer group
# numericthreshnumber= risk threshold number for numeric values, to determine peer group - any value above this number is not in peer group
# lag=number of lags to smooth the value for outlier detection
# zthresh=number of standard deviations for data
# influence=A number between 0-1, where 1=normal inflence and 0.5 is half

def genflagstraining(allstreams,stringthreshnumber=0.1,numericthreshnumber=0.1,lag=5,zthresh=2.5,influence=0.5):

 #flags="""topic=viperdependentvariable,topictype=numeric,threshnumber=300,lag=5,zthresh=2.5,
  #    influence=0.5~topic=viperindependentvariable1,topictype=numeric,threshnumber=300,lag=5,zthresh=2.5,
   #   influence=0.5~topic=viperindependentvariable2,topictype=numeric,threshnumber=300,lag=5,zthresh=2.5,
    #  influence=0.9~topic=textdata1,topictype=string,threshnumber=10~topic=textdata2,topictype=string,
     # threshnumber=.80"""
       
    buf=""
    streamflags=""
    
    streamlist=allstreams.split(",")
    for s in streamlist:
         if 'amountpaid' in s: #numeric
              buf=buf+"topic=%s,topictype=numeric,threshnumber=%.3f,lag=%d,zthresh=%.2f,influence=%.2f~" % (s,numericthreshnumber,lag,zthresh,influence)
         else:
              buf=buf+"topic=%s,topictype=string,threshnumber=%.3f~" % (s,stringthreshnumber)
  
    buf=buf[:-1]
    streamflags=streamflags+buf          
    return streamflags          

# Function inputs
# NOTE: We now generate flags for the predictions that allows users to control the sensitivity of the anomalies when compared against
# the peer-group
# allstreams =all streams to check for anomalies
# overallriskscore= risk threshold - if computed risk exceeds or equals this then it is flagged as anomalous
# completeandor=if 'or' then if any stream exceeds the overallriskscore the transaction is flagged
# numvaluetype= if the real-time transaction value it less than, equal or greater than numvalue it is flagged
# stringvaluetype=if the string stream has this value it is flagged
# numericscore=if numeric score exceeds this value when comparing transactions to peer group - then it is flagged
# stringscore=if string score exceeds this value when compared to peer group - then it is flagged
# stringcontains= if 0, then if string value specified transaction values are equated to this string value otherwise 
#                 if 1, then string can be a subset of the transaction value
# numericlogictype=if or, then either numeric value or score can trigger a flag on the transactions for anomaly
#                  if and, then both must trigger the flag 
# stringlogictype=if or, then either string value or score can trigger a flag on the transactions for anomaly
#                 if and, then both must trigger the flag  
def genflagsprediction(allstreams,overallriskscore,completeandor,numvaluetype,numericlogictype,stringvaluetype,stringlogictype,
             numericscore,stringscore,stringcontains):

#    flags="""flags=riskscore=.4~complete=or~type=or,topic=viperdependentvariable,topictype=numeric,
 #     sc>500~type=and,topic=viperindependentvariable1,topictype=numeric,v1<100,sc>100~
  #    type=or,topic=textdata1,topictype=string,stringcontains=1,v2=valueany,sc>.6~type=or,
   #   topic=textdata2,topictype=string,stringcontains=0,v2=Failed Record^Failed Record^test record,
    #  sc>.210~type=or,topic=viperindependentvariable2,topictype=numeric,v1<100,sc>1000"""

    streamflags="flags=riskscore=%.2f~complete=%s~" % (overallriskscore,completeandor)
       
    buf=""
    streamlist=allstreams.split(",")
    for s in streamlist:
         if 'amountpaid' in s: #numeric
              buf=buf+"type=%s,topic=%s,topictype=numeric,v1%s,sc>%.3f~" % (numericlogictype,s,numvaluetype,numericscore)
         else:
              buf=buf+"type=%s,topic=%s,topictype=string,stringcontains=%d,v2%s,sc>%.3f~" % (stringlogictype,s,stringcontains,stringvaluetype,stringscore)
  
    buf=buf[:-1]
    streamflags=streamflags+buf          
    return streamflags 

def performAnomalyDetection(streamstoanalyse,flagstraining,flagsprediction,bankaccount,maintopic):
      #############################################################################################################
      #                                     JOIN DATA STREAMS 
      # Set personal data
      companyname="OTICS Advanced Analytics"
      myname="Sebastian"
      myemail="Sebastian.Maurice"
      mylocation="Toronto"

      joinedtopic="otics-tmlbook-joined-bankaccount-streams" #+str(bankaccount)
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

      ############################################################################################################
      #                                     SETUP TO PREDICT ANOMALIES

      # Assign the name of the topic to consume the peer groups from
      #consumefrom = "otics-tmlbook-anomalypeergroup-"+str(bankaccount)
      consumefrom = "otics-tmlbook-anomalypeergroup"
      

      # Create a topic to store the anomaly results to- USE THIS TOPIC (anomalydataresults)
      # FOR VIPERviz visualization
      
      produceto="otics-tmlbook-anomalydataresults"
      description="Topic to store the anomaly results"
      result=maadstml.vipercreatetopic(VIPERTOKEN,VIPERHOST,VIPERPORT,produceto,companyname,myname,
                                     myemail,mylocation,description,enabletls,
                                     brokerhost,brokerport,numpartitions,replication,microserviceid)

      # Create another topic to store the peer groups for anomaly prediction
      #peergrouptotopic="otics-tmlbook-anomalypeergroup-"+str(bankaccount)
      peergrouptotopic="otics-tmlbook-anomalypeergroup"#+str(bankaccount)
      
      result=maadstml.vipercreatetopic(VIPERTOKEN,VIPERHOST,VIPERPORT,peergrouptotopic,companyname,
                                    myname,myemail,mylocation,description,enabletls,
                                    brokerhost,brokerport,numpartitions,replication,microserviceid)

      ############################################################################################################
      #                                     START ANOMALY PREDICTION
      # name the topic to produce peer group to
      producepeergroupto = peergrouptotopic
      
      # Consume from the Peer Group Topic you created in viperanomalytrain
      consumefrom=producepeergroupto

      # Consume from the stream containing the new transactions
      consumeinputstream=''
      
      # Name the topic to produce to
      consumeridproduceto=''
      # Streams to analyse - we are analysing 5 streams - you can use any amount of streams
     

      # Assign variables
      consumerid=''
      producerid=''

      produceridinputstreamtest=''
      consumeridinputstream=''



      # Start Anomaly Predicting:
      # 1) To predict anomalies you use the Peer Groups created by viperanomalytrain and stored in producepeergroupto
      # 2) You MUST consumefrom the producepeergroupto topic i.e. consumefrom=producepeergroupto
      # 3) You consume the joined input streams i.e. joinedtopic created by viperproducetotopicstream
      # Fields: 
      # consumefrom= producepeergroupto; consume the peer groups 
      # produceto= produce the anomaly predictions to a topic; use this topic to Visualize the anomalies
      # consumeinputstream= this is the input stream of NEW transactions from joinedtopic created by viperproducetotopicstream
      # produceinputstreamtest= produce the formatted input stream to a topic
      # produceridinputstreamtest= producer id for the input stream
      # streamstoanalyse= joined streams to analyse - these are the streams in the consumefrom topic
      # consumeridinputstream= consumer id for the input stream
      # companyname= company name
      # consumeridmainpredict= consumerid for the anomaly RESULTS topic
      # producerid= producerid of the produceto topic
      # flagsprediction= flags to deteremine if a transaction is anomalous
      # hpdehost= hpde host address
      # hpdeport= hpde port number
      # viperconfigfile= Viper configuration file
      # enabletls= 1, if SSL/TLS is enabled in Kafka, 0 if its not enabled
      # peergroup_partition= partition of the peer group from viperanomalytrain

      consumeinputstream=''
      produceinputstreamtest=''
      produceridinputstreamtest=''
      consumeridinputstream=''
      consumeridmainpredict=''
      producerid=''
      peergroup_partition=''
      topicid=bankaccount
      rollbackoffsets=50
      fullpathtopeergroupdata='c:/viperdemo/models/anomaly'

      ###################### ONLY ONE LINE OF CODE NEEDED TO PREDICT ANOMALIES ON ANY NUMBER OF DATA STREAMS ###################     

      result2=maadstml.viperanomalypredict(VIPERTOKEN,VIPERHOST,VIPERPORT,consumefrom,produceto,
                                        consumeinputstream,produceinputstreamtest,
                                        produceridinputstreamtest, streamstoanalyse, 
                                        consumeridinputstream,companyname,consumeridmainpredict,
                                        producerid,flagsprediction,hpdehost,viperconfigfile,enabletls,
                                        peergroup_partition,hpdeport,topicid,maintopic,rollbackoffsets,fullpathtopeergroupdata)

      print(result2)

##########################################################################
def checkaccounts(k,maintopic,streamstoanalyse):
     
     try:
            
       ######################################### FLAGS FOR CONSTRUCTING PEER GROUPS #######################################

       # stringthreshnumber=0.97 - strings are counted for number of occurences - more repetition of a string the more common it is
       #     therefore, if a string similarity value is below stringthreshnumber it will be in the peer group.  You can adjust this
       #     value based on your data.  Keep it between 0-1.
       stringthreshnumber=0.95
       
       # numericthreshnumber=0.09- this will perform Z-score analysis on the data and remove any outliers, then it will perform
       #     standarized tests on each numeric value to determine normality.  If this normality test values are below numericthreshnumber
       #     then it will be in the peer group. You can adjust this value based on your data.  Keep it between 0-1.
       numericthreshnumber=0.09
       
       # lag=5 - this is the smoothing factor for Z-score analysis, normally 5 is fine
       lag=5
       
       # zthresh=2.5 - this is the number of standard deviations of the data from the mean - you can adjust this number.
       zthresh=2.5
       
       # influnence=0.5 - this is for z-score analysis - you can adjust this number between 0-1 usually 0.5 is good.
       influence=0.5
       
       flagstraining=genflagstraining(streamstoanalyse,stringthreshnumber,numericthreshnumber,lag,zthresh,influence)

       ######################################### FLAGS FOR PREDICTING ANOMALIES #######################################

       # overallriskscore- This is the overall risk threshold from all of the streams.  For example, if you are checking a Bank account with 5 streams
       #   such are name, amount paid, product purchased, location, counterparty - then each stream will be checked for anomalous values
       #   The combined risk score will be compared against the overallriskscore.
       overallriskscore=0.51

       # completeandor - this tell VIPER to see of all streams have a risk level that exceeds the overallriskscore (completeandor="and"), or
       #             if atleast one stream exceeds the overallriskscore (completeandor="or")
       completeandor="or"

       # numvaluetype - you can specify a value to test for.  For example, >4000 means to flag the stream of transactions if it exceeds 4000,
       #         you can also use < (less than) you can also specify "valueany"
       numvaluetype=">4000"

       # numericlogictype - this can be "and" "or" and is used to test if the numvaluetype exists "and" stream has an anomalous entry.
       #              For example, if numericlogictype="and" then the numvalue exceeds a value AND the stream contains an anomaly                
       numericlogictype="or"

       # stringvaluetype - this will check the stream of transactions for a string value - if you want to check for a specific string value
       #       like "error" - you can specify it here.  You can you ^ (and) and | (or).  For example, if error1 and error2 use "=error1^error2"
       #       You can also specify "=valueany" 
       stringvaluetype="=valueany"

       # stringlogictype - this is similar to numericlogictype, and will test one or both to see if the string value exists and the stringscore
       # exceed the value
       stringlogictype="or"

       # numericscore - this is the risk score threshold for numeric streams.  A risk value exceeding this value is flagged.
       numericscore=0.09

       # stringscore - this is the risk score threshold for strings streams.  A risk value exceeding this value is flagged.
       stringscore=0.90

       # stringcontains - this deterimes whether to do a substring comparison of new strings to their peers (stringcontains=1) or
       #             or not (stringcontains=0, then equate).   
       stringcontains=1
              
       flagsprediction=genflagsprediction(streamstoanalyse,overallriskscore,completeandor,numvaluetype,numericlogictype,stringvaluetype,stringlogictype,
             numericscore,stringscore,stringcontains)
       
       performAnomalyDetection(streamstoanalyse,flagstraining,flagsprediction,k,maintopic)

       
     except Exception as e:
       print(e)   
       pass   


# Checking 10 Bank accounts for Anomaly
numberofbankaccounts=10

# Keep checking 10,000 times - you can change this to any number or infinite loop
numanomalyruns=100
maintopic="otics-tmlbook-anomaly-mainstream"
streamstoanalyse=streamstocheckforanomalies()
     

#cpus=multiprocessing.cpu_count()
for j in range(numanomalyruns):
   for k in range(numberofbankaccounts):
    element_run = Parallel(n_jobs=1)(delayed(checkaccounts)(k,maintopic,streamstoanalyse) for k in range(numberofbankaccounts))  
