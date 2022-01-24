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

def streamstocheckforanomalies(bankaccount):
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

def performAnomalyDetection(streamstojoin,flagstraining,flagsprediction,bankaccount,maintopic):
      #############################################################################################################
      #                                     JOIN DATA STREAMS 
      # Set personal data
      companyname="OTICS Advanced Analytics"
      myname="Sebastian"
      myemail="Sebastian.Maurice"
      mylocation="Toronto"

      # Joined topic name
      streamlist=streamstojoin.split(",")
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

      topicid=bankaccount


      
      #############################################################################################################
      #                                     SETUP TOPICS FOR PEER GROUP ANALYSIS

      description="Topic needed for peer group analysis"
      # Create a topic that will store peer group data
      #producetotopic="otics-tmlbook-anomalytestdata-"+str(bankaccount)
      producetotopic="otics-tmlbook-anomalytestdata"#+str(bankaccount)

      result=maadstml.vipercreatetopic(VIPERTOKEN,VIPERHOST,VIPERPORT,producetotopic,companyname,myname,
                                     myemail,mylocation,description,enabletls,
                                     brokerhost,brokerport,numpartitions,replication,microserviceid)


      # Create another topic to store the peer groups for anomaly prediction
      #peergrouptotopic="otics-tmlbook-anomalypeergroup-"+str(bankaccount)
      peergrouptotopic="otics-tmlbook-anomalypeergroup"#+str(bankaccount)
      
      result=maadstml.vipercreatetopic(VIPERTOKEN,VIPERHOST,VIPERPORT,peergrouptotopic,companyname,
                                    myname,myemail,mylocation,description,enabletls,
                                    brokerhost,brokerport,numpartitions,replication,microserviceid)

     
      #############################################################################################################
      #                                     START ANOMALY TRAINING 
      # name the topic to produce to
      produceto = producetotopic

      # name the topic to produce peer group to
      producepeergroupto = peergrouptotopic

      # Assign the producer id of the peer group topic      
      produceridpeergroup=''

      # Assign the consumer id of the produceto topic
      consumeridproduceto=''

      # Identify the streams to analyse for Anomalies
      streamstoanalyse=streamstojoin

      # Assign the consumer id of the topic you are consuming the data for peer group analysis
      consumerid=''

      # Assign the producer id you want to produce results to 
      producerid=''

      # Enable SSL/TLS
      enabletls=1

      # Assign the partition you extracted from the function: viperproducetotopicstream
      partition=''
      
      # Start Anomaly training:
      # 1) To build a trainign dataset for anomaly prediction, you first create a Peer Group of transactions from
      #    the input stream produced by viperproducetotopicstream
      # 2) You store the peer group in the topic producepeergroupto
      # 3) Use the peer group topic in the "consumefrom" field to predict anomalies
      # Fields: 
      # consumefrom= joinedtopic; consumefrom should be used in "consumeinputstream" in viperanomalypredict
      # produceto= this is an intermediary topic needed to process the joinedtopic
      # producepeergroupto= otics-tmlbook-anomalypeergroup; producepeergroupto should be used in the "consumefrom" in viperanomalypredict
      # produceridpeergroup= producer id for the peer group
      # consumeridproduceto= consumer id for the produceto topic
      # streamstoanalyse= joined streams to analyse - these are the streams in the consumefrom topic
      # companyname= company name
      # consumerid= consumerid of the consumefrom topic
      # producerid= producerid of the produceto topic
      # flagstraining= flags to use to create the peer group
      # hpdehost= hpde host address
      # hpdeport= hpde port number
      # viperconfigfile= Viper configuration file
      # enabletls= 1, if SSL/TLS is enabled in Kafka, 0 if its not enabled
      # partition= parition of the input stream, returned by viperproducetotopicstream
      # rollback offset by 50%
      rollbackoffsets=50
      #leave blank
      consumefrom=''
      #leave blank
      produceridpeergroup=''
      #leave blank
      consumeridproduceto=''
      #leave blank
      consumerid=''
      #leave blank
      producerid=''
      #leave blank
      partition=''
      #change to path to keep peer group data
      fullpathtotrainingdata='c:/viperdemo/models/anomaly'

      ###################### ONLY ONE LINE OF CODE NEEDED TO FIND PEER-GROUP FOR ANOMALY DETECTION ON ANY NUMBER OF DATA STREAMS ###################     
      result=maadstml.viperanomalytrain(VIPERTOKEN,VIPERHOST,VIPERPORT,consumefrom,produceto,
                              producepeergroupto,produceridpeergroup,
                              consumeridproduceto, streamstoanalyse,companyname,consumerid,
                              producerid,flagstraining,hpdehost,viperconfigfile, enabletls,partition,
                              hpdeport,topicid,maintopic,rollbackoffsets,fullpathtotrainingdata)

      print("Result=",result)
      
      print("PEER Group will be produced to topic=", producepeergroupto)

      

##########################################################################
def checkaccounts(maintopic,k):
     
     try:
       joinedstreams=streamstocheckforanomalies(k)
       print(joinedstreams)

            
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
       
       flagstraining=genflagstraining(joinedstreams,stringthreshnumber,numericthreshnumber,lag,zthresh,influence)

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
              
       flagsprediction=genflagsprediction(joinedstreams,overallriskscore,completeandor,numvaluetype,numericlogictype,stringvaluetype,stringlogictype,
             numericscore,stringscore,stringcontains)
       
       #print(flagstraining)
       #print(flagsprediction)
       performAnomalyDetection(joinedstreams,flagstraining,flagsprediction,k,maintopic)
       #time.sleep(1)

       
     except Exception as e:
       print(e)   
       pass   

# Perform Anomaly detection on 10 Bank accounts - this could be any number
numberofbankaccounts=10

# Keep checking 10,000 times - you can change this to any number or infinite loop
numanomalyruns=100
maintopic="otics-tmlbook-anomaly-mainstream"

for j in range(numanomalyruns):
   for k in range(numberofbankaccounts):     
     element_run = Parallel(n_jobs=1)(delayed(checkaccounts)(maintopic,k) for k in range(numberofbankaccounts))


