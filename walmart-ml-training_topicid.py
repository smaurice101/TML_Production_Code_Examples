# Developed by: Sebastian Maurice, PhD
# Company: OTICS Advanced Analytics Inc.
# Date: 2022-01-18 
# Toronto, Ontario Canada
# For help email: support@otics.ca 

#######################################################################################################################################

# This file will perform TML for Walmart Foot Traffic.  Before using this code you MUST have:
# 1) Downloaded and installed MAADS-VIPER from: https://github.com/smaurice101/transactionalmachinelearning

# 2) You have:
# a) VIPER listening for a connection on port IP: http://127.0.01 and PORT: 8000 (you can specify different IP and PORT
#    just change the  VIPERHOST="http://127.0.0.1" and VIPERPORT=8000)
# b) HPDE listening for a connection on port IP: http://127.0.01 and PORT: 8001 (you can specify different IP and PORT
#    just change the  hpdehost="http://127.0.0.1" and hpdeport=8001)

# 3) You have created a Kafka cluster in Confluent Cloud (https://confluent.cloud/)

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

# Import the core libraries
import maadstml

# Uncomment IF using jupyter notebook
#import nest_asyncio

import json
import time
from joblib import Parallel, delayed

# Uncomment IF using jupyter notebook
#nest_asyncio.apply()

# Set Global variables for VIPER and HPDE - You can change IP and Port for your setup of 
# VIPER and HPDE
VIPERHOST="https://127.0.0.1"
VIPERPORT=8000
hpdehost="http://127.0.0.1"
hpdeport=8001

# Set Global variable for Viper confifuration file - change the folder path for your computer
viperconfigfile="c:/maads/golang/go/bin/viper.env"

#############################################################################################################
#                                      STORE VIPER TOKEN
# Get the VIPERTOKEN from the file admin.tok - change folder location to admin.tok
# to your location of admin.tok
def getparams():
        
     with open("c:/maads/golang/go/bin/admin.tok", "r") as f:
        VIPERTOKEN=f.read()
  
     return VIPERTOKEN

VIPERTOKEN=getparams()


def performSupervisedMachineLearning(maintopic,topicid):

#############################################################################################################
#                                     SETUP INITIAL CONFIGURATION FOR KAFKA AND VIPER

      # Set personal data
      companyname="OTICS Advanced Analytics"
      myname="Sebastian"
      myemail="Sebastian.Maurice"
      mylocation="Toronto"

      # Joined topic name
      joinedtopic="otics-tmlbook-walmartretail-foottrafic-prediction-joinedtopics-input"
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

      description="Topic containing joined streams for Machine Learning training dataset"

      streamstojoin=["otics-tmlbook-walmartretail-foottrafic-prediction-foottrafficamount-input","otics-tmlbook-walmartretail-foottrafic-prediction-hourofday-input",
              "otics-tmlbook-walmartretail-foottrafic-prediction-monthofyear-input","otics-tmlbook-walmartretail-foottrafic-prediction-walmartlocationnumber-input"]

      streamstojoin=','.join(streamstojoin)
      # Call MAADS python function to create joined stream topic


      #############################################################################################################
      #                         CREATE TOPIC TO STORE TRAINED PARAMS FROM ALGORITHM  
      
      producetotopic="otics-tmlbook-walmartretail-foottrafic-prediction-trained-params-input"

      description="Topic to store the trained machine learning parameters"
      result=maadstml.vipercreatetopic(VIPERTOKEN,VIPERHOST,VIPERPORT,producetotopic,companyname,
                                     myname,myemail,mylocation,description,enabletls,
                                     brokerhost,brokerport,numpartitions,replication,
                                     microserviceid='')

      #############################################################################################################
      #                         VIPER CALLS HPDE TO PERFORM REAL_TIME MACHINE LEARNING ON TRAINING DATA 


      producetotopic="otics-tmlbook-walmartretail-foottrafic-prediction-trained-params-input"
      # deploy the algorithm to ./deploy folder - otherwise it will be in ./models folder
      deploy=1
      # number of models runs to find the best algorithm - higher numbers may find better models but consume more memory
      modelruns=7
      # Go to the last offset of the partition in partition_training variable
      offset=-1
      # If 0, this is not a logistic model where dependent variable is discreet
      islogistic=0
      # set network timeout for communication between VIPER and HPDE in seconds
      # increase this number if you timeout
      networktimeout=600

      # This parameter will attempt to fine tune the model search space - a number close to 0 means you will have lots of
      # models but their quality may be low.  A number close to 100 means you will have fewer models but their predictive
      # quality will be higher.
      modelsearchtuner=90

      #this is the dependent variable
      dependentvariable="otics-tmlbook-walmartretail-foottrafic-prediction-foottrafficamount-input"
      # Assign the independentvariable streams
      independentvariables=["otics-tmlbook-walmartretail-foottrafic-prediction-hourofday-input",
              "otics-tmlbook-walmartretail-foottrafic-prediction-monthofyear-input","otics-tmlbook-walmartretail-foottrafic-prediction-walmartlocationnumber-input"]
      independentvariables=','.join(independentvariables)
      # Rollback offset by 1% 
      rollbackoffsets=1
      # leave blank
      consumeridtrainingdata2=''
      # leave blank
      partition_training=''
      # leave blank
      producerid=''
      # leave blank
      consumefrom=''
      #change to any folder - this is useful if you have different customers
      fullpathtotrainingdata='c:/viperdemo/models/walmart'

      ###################### ONLY ONE LINE OF CODE NEEDED TO BUILD MULTIPLE ML MODELS FOR EACH LOCATION/DEVICE ###################
      result=maadstml.viperhpdetraining(VIPERTOKEN,VIPERHOST,VIPERPORT,consumefrom,producetotopic,
                                      companyname,consumeridtrainingdata2,producerid, hpdehost,
                                      viperconfigfile,enabletls,partition_training,
                                      deploy,modelruns,modelsearchtuner,hpdeport,offset,islogistic,
                                      brokerhost,brokerport,networktimeout,microserviceid,topicid,maintopic,
                                      independentvariables,dependentvariable,rollbackoffsets,fullpathtotrainingdata)    
##########################################################################

# Change this to any number - this represents any number of devices, people, products, accounts, locations, etc.
# Here we are building/training individual ML models 10 Walmart locations AUTOMATICALLY
locations=10
maintopic="otics-tmlbook-walmartretail-mainstream"

# create separate and custom micro-ML models for each account, location, IoT device, etc.
element_run = Parallel(n_jobs=1)(delayed(performSupervisedMachineLearning)(maintopic,j) for j in range(locations))
