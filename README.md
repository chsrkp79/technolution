# technolution
jackie chan robot

####### Data Consumer 
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2  JackieGame.py 127.0.0.1:9092 jackieChanCommand

#### Producer
 java -jar json-data-generator-1.2.1.jar jackieChanSimConfig.json
 
####  Logger file 
 
logs/logdata-04-05-2017-1.log

#### Sample Output 

```
#############################################################
#################JackieChan Robot Game Results###############
Killing weapon ROPE
Favourite style DRUNKEN_BOXING
Moves count 3
Maximum action used PUNCH
#############################################################
```
