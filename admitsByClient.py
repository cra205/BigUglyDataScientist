# To run this example navidate to the bin folder of the spark home and use:
# ./bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.10:1.0.0 admitsByClient.py

# import packages

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

# configure Spark Configuration and Context

if __name__ == "__main__":

    sparkConf = SparkConf().setMaster("yarn-client") \
                           .setAppName("admitsByClient") \
                           .set("spark.app.id", "MongoSparkConnectorTour")
            
    sc = SparkContext(conf=sparkConf)
    sqlContext = SQLContext(sc)
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.FATAL)


    # create pipeline object
    
    pipeline = [
                {'$match': {'projectCode':'R3', }},
                {'$unwind':'$admitsByClient'}, 
                {'$project':{'_id':0,'admitsByClient.clientName':1,'hospID':1,'admitsByClient.clientAdmits':1,'admitsByClient.clientStatus':1}},
                {'$group': {'_id': '$admitsByClient.clientName', 'total': { '$sum':'$admitsByClient.clientAdmits' }}}
               ] 
    
    
    # pull data from mongo
    
    admitsByClient = sqlContext.read.format("com.mongodb.spark.sql.DefaultSource") \
                                    .option("spark.mongodb.input.uri","mongodb://dylan:abrams@apsrd6777.uhc.com:27017/admin") \
                                    .option("spark.mongodb.input.database","hdc") \
                                    .option("spark.mongodb.input.collection","hdc_chase") \
                                    .option("pipeline", pipeline) \
                                    .load()
    
    
    # display dataframe in memory before saving to mongo
    
    print("data frame in memory before saving")
    admitsByClient.show()
    
    
    # save admits by client dataframe to mongo
    
    admitsByClient.write.format("com.mongodb.spark.sql.DefaultSource") \
                        .option("spark.mongodb.output.uri","mongodb://dylan:abrams@apsrd6777.uhc.com:27017/admin") \
                        .option("spark.mongodb.output.database","sandbox") \
                        .option("spark.mongodb.output.collection","pythontest2") \
                        .mode("overwrite").save()


    # read the data from MongoDB and Load into a spark dataframe
    
    df = sqlContext.read.format("com.mongodb.spark.sql.DefaultSource") \
                        .option("spark.mongodb.input.uri","mongodb://dylan:abrams@apsrd6777.uhc.com:27017/admin") \
                        .option("spark.mongodb.input.database","sandbox") \
                        .option("spark.mongodb.input.collection","pythontest2").load()
    
    # display dataframe after loading into spark from mongodb
    
    print("data frame read from MongoDB")
    df.show()

    
    # stop spark context
    sc.stop()
