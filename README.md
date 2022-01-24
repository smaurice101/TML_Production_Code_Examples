# Transactional Machine Learning : Produce Code Examples*
*If you are writing TML solutions for production for thousands of IoT devices, products, people, etc..this code will show you how you can use ONE line of code to perform: ML Training, Predictions, Optimizations, Anomaly detections. 

This code will also show you how you can create "sub-streams" have streaming into 1 topic, and reduce your cloud costs significantly.  For example, even if you want to create inidividual ML models for 10,000 IoT devices, you do NOT need 10,000 streams, rather you can create 1 stream, and create 10,000 sub-streams inside the 1 stream.  This way you will be charged for 1 topic, not 10,000 topics or partitions.

To begin on the Walmart example: 
**1. Run the produce-data_walmart-topicid.py**: this is will produce data to a main stream and add sub-streams 
 
**2. ML Training**: Run the code *walmart-ml-training_topicid.py* - this will create invidual ML models for each Walmart location.

**3. ML Predictions**: Run the code *walmart-ml-prediction_topicid.py* - this will generate predictions using the ML models you created for each location.

**3. ML Optimization**: Run the code *walmart-ml-optimization_topicid.py* - this will perform mathematical optimizations using the ML models/predictions you created for each location.

To begin on the Anomaly Detection example: 
**1. Run the produce-data_walmart-topicid.py**: this is will produce data to a main stream and add sub-streams 
 
**2. ML Training**: Run the code *walmart-ml-training_topicid.py* - this will create invidual ML models for each Walmart location.

**3. ML Predictions**: Run the code *walmart-ml-prediction_topicid.py* - this will generate predictions using the ML models you created for each location.

**3. ML Optimization**: Run the code *walmart-ml-optimization_topicid.py* - this will perform mathematical optimizations using the ML models/predictions you created for each location.


