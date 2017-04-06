from __future__ import print_function

import sys
import json
import ast
import yaml
import operator
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

head = 20
legs_arms = 50
body = 30
actions_dct = {}
styles_dct = {}
killing_weapon = {}
game_over = 0
moves_count = 0
def perform_action(rdd):
    """Performing actions on set of incoming stream RDDs"""
    global moves_count, dct, head, legs_arms, body, game_over
    if (game_over == 1):
        return
    top_list = rdd.take(1)
    # Process each of the RDD
    for x in top_list:
        x = x.replace('"', r"'")
        data = "\"" + x + "\""
        #print (ast.literal_eval(data), type(ast.literal_eval(data)))
        print ("#########")
        # Convert the Stream type data to dictionary
        str = ast.literal_eval(data)
        d = yaml.load(x)
        data_dct = {}

        # Parse the data and keep in dictionary for processing
        for key, value in d.iteritems():
            data_dct[key] = value
            print (key, value)

        # Business logic as the problem statement: JackieChanRobot
        for key, value in data_dct.iteritems():
            if ((key == "action") and (value == "BLOCK" or value == "JUMP")):
                break
            if (key == "action" and (value == "KICK" or value == "PUNCH")):
                moves_count =  moves_count + 1
                if actions_dct.has_key(value):
                    actions_dct[value] += 1
                else:
                    actions_dct[value] = 1
            if (key == "style"):
                if styles_dct.has_key(value):
                    styles_dct[value] += 1
                else:
                    styles_dct[value] = 1
            if (key == "target" and value == "HEAD"):
                head = head - data_dct['strength']
                if head <= 0:
                    game_over = 1
                    killing_weapon = data_dct['weapon']
                    break
            if (key == "target" and (value == "LEGS" or value == "ARMS")):
                legs_arms = legs_arms - data_dct['strength']
                if legs_arms <= 0:
                    game_over = 1
                    killing_weapon = data_dct['weapon']
                    break
            if (key == "target" and value == "BODY"):
                body = body - data_dct['strength']
                if body <= 0:
                    game_over = 1
                    break
    if (game_over == 1):
        print ("\n\n")
        print ("#############################################################")
        print ("#################JackieChan Robot Game Results###############")
        print ("Killing weapon", killing_weapon)
        print ("Favourite style", max(styles_dct, key=styles_dct.get))
        print ("Moves count", moves_count)
        print ("Maximum action used", max(actions_dct, key=actions_dct.get))
        print ("#############################################################")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: JackieGame.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="JackieChan")
    ssc = StreamingContext(sc, 1)
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1])
    lines.foreachRDD(perform_action)

    ssc.start()
    ssc.awaitTermination()
