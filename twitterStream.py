from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.legend_handler import HandlerLine2D


def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
    
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    positives = []
    negatives = []
    for eachList in counts:
        for eachRow in eachList:
            if eachRow[0] == 'positive':
                positives.append(eachRow[1])
            elif eachRow[0] == 'negative':
                negatives.append(eachRow[1])   
        
    
    positives.insert(0, None)
    negatives.insert(0, None)
    
    timesteps = np.arange(0, len(positives), 1) 
    
    p, = plt.plot(timesteps, positives, 'bo-', label = "positive")
    n, = plt.plot(timesteps, negatives, 'go-', label = "negative")
    plt.legend(handler_map={p: HandlerLine2D(numpoints=2)}, loc=2) #http://matplotlib.org/users/legend_guide.html
    plt.xticks(timesteps)
    plt.yticks(np.arange(0, max(max(positives), max(negatives)) + 100, 50)) # give extra space on y axis by 100 to show legend so that it does not cut the plot
    plt.xlabel("Time step")
    plt.ylabel("Word count")
    plt.show()
    return



def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    wordList = []
    with open(filename, 'r') as wordFile:
        for line in wordFile:
            wordList.append(line.strip("\n"))
    wordFile.close()
    return wordList        



def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    words = tweets.flatMap(lambda tweet: tweet.split(" "))
    words = words.filter(lambda word: word in pwords or word in nwords)
    pairs = words.map(lambda word: ("positive", 1) if word in pwords else ("negative", 1))
    wordCounts = pairs.reduceByKey(lambda x,y: x+y)
    #wordCounts.pprint()
    
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    wordCounts.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    runningCounts = pairs.updateStateByKey(updateFunction)
    runningCounts.pprint()

    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


def updateFunction(newValues, runningCount):
    if runningCount is None:
       runningCount = 0
    return sum(newValues, runningCount)  #add the new values with the previous running count to get the new count


if __name__=="__main__":
    main()
