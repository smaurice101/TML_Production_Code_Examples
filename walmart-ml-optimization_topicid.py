# Developed by: Sebastian Maurice, PhD
# Company: OTICS Advanced Analytics Inc.
# Date: 2021-01-18 
# Toronto, Ontario Canada
# For help email: support@otics.ca 

#######################################################################################################################################

# This file will predict and optimize Walmart Foot Traffic.  Before using this code you MUST have:
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


def performPredictionOptimization(maintopic,topicid):

#############################################################################################################
#                                     JOIN DATA STREAMS 

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


      #############################################################################################################
      #                         CREATE TOPIC TO STORE PREDICTIONS FROM ALGORITHM  

      producetotopic="otics-tmlbook-walmartretail-foottrafic-prediction-results-output"
      description="Topic to store the predictions"
      result=maadstml.vipercreatetopic(VIPERTOKEN,VIPERHOST,VIPERPORT,producetotopic,companyname,
                                    myname,myemail,mylocation,description,enabletls,
                                    brokerhost,brokerport,numpartitions,replication,
                                    microserviceid)     

      #############################################################################################################
      #                         CREATE TOPIC TO STORE OPTIMAL PARAMETERS FROM ALGORITHM  

      producetotopic="otics-tmlbook-walmartretail-foottrafic-optimization-results-output"
      description="Topic for optimization results"
      result=maadstml.vipercreatetopic(VIPERTOKEN,VIPERHOST,VIPERPORT,producetotopic,companyname,
                                    myname,myemail,mylocation,description,enabletls,
                                    brokerhost,brokerport,numpartitions,replication,
                                    microserviceid='')
      print(result)

      #############################################################################################################
      #                                     START MATHEMATICAL OPTIMIZATION FROM ALGORITHM
      consumefrom="otics-tmlbook-walmartretail-foottrafic-prediction-trained-params-input"
      delay=10000
      offset=-1
      # we are doing minimization if ismin=1, otherwise we are maximizing the objective function
      ismin=0
      # choosing constraints='best' will force HPDE to choose the constraints for you
      constraints='best'
      # We are going to expand the lower and upper bounds on the constraints by 20%
      stretchbounds=50
      # we are going to use MIN and MAX for the lower and upper bounds on the constraints
      constrainttype=1
      # We are going to see if there are 'better' optimal values around an epsilon distance (10%)
      # from the local optimal values found
      epsilon=20
      # network timeout in seconds between VIPER and HPDE
      timeout=120
      # leave blank
      producerid=''
      # leave blank
      produceridhyperprediction=''
      # leave blank
      consumeridtraininedparams=''
      # leave blank
      groupid=''
      # Use model not in /deploy folder - you can switch between deploy or not this is useful if you are testing models      
      usedeploy=0

############################################ ONLY ONE LINE OF CODE TO OPTIMIZE FOR MULTIPLE DEVICES AT ONCE #########################            
      result7=maadstml.viperhpdeoptimize(VIPERTOKEN,VIPERHOST,VIPERPORT,consumefrom,producetotopic,
                                      companyname,consumeridtraininedparams,
                                      producerid,hpdehost,-1,
                                      offset,enabletls,delay,hpdeport,usedeploy,ismin,
                                      constraints,stretchbounds,constrainttype,epsilon,
                                      brokerhost,brokerport,timeout,microserviceid,topicid)
      print(result7)

##########################################################################

# Change this to any number - this represents any number of devices, people, products, accounts, locations, etc.
# Here we are doing matheatical optimizationt to find optimal values for the independent variables that maximize
# foot traffic for each individual Walmart location AUTOMATICALLY
locations=10
maintopic="otics-tmlbook-walmartretail-mainstream"

element_run = Parallel(n_jobs=1)(delayed(performPredictionOptimization)(maintopic,j) for j in range(locations))


