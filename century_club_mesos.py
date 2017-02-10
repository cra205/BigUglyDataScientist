# import necessary packages

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext


# set configuration and spark context

if __name__ == "__main__":

    sparkConf = SparkConf().setMaster("local") \
                           .setAppName("MongoSparkConnectorTour") \
                           .set("spark.app.id", "MongoSparkConnectorTour")
            
    sc = SparkContext(conf=sparkConf)
    sqlContext = SQLContext(sc)
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.FATAL)

    # create RDD of characters and their ages
    
    charactersRdd = sc.parallelize([("Bilbo Baggins",  50), ("Gandalf", 1000), ("Thorin", 195), ("Balin", 178), ("Kili", 77),
                                    ("Dwalin", 169), ("Oin", 167), ("Gloin", 158), ("Fili", 82), ("Bombur", None)])
    
    
    # creat data from from charactersRDD
    
    characters = sqlContext.createDataFrame(charactersRdd, ["name", "age"])
    
    
    # save characters dataframe to MongoDB
    
    characters.write.format("com.mongodb.spark.sql.DefaultSource") \
                    .option("spark.mongodb.output.uri","mongodb://admin:password@10.106.172.42:14606/ois") \
                    .option("spark.mongodb.output.database","ois") \
                    .option("spark.mongodb.output.collection","sparkConnectorTest") \
                    .mode("overwrite") \
                    .save()


    # read the data from MongoDB into Spark and load it into a dataframe
    
    df = sqlContext.read.format("com.mongodb.spark.sql.DefaultSource") \
                        .option("spark.mongodb.input.uri","mongodb://dylan:admin:password@10.106.172.42:14606/ois") \
                        .option("spark.mongodb.input.database","ois") \
                        .option("spark.mongodb.input.collection","sparkConnectorTest") \
                        .load()
    
    # display the schema of the dataframe
    
    print("Schema:")
    df.printSchema()

    
    # Register dataframe as a temp table and perform a query against it
    
    df.registerTempTable("characters")
    centenarians = sqlContext.sql("SELECT name, age FROM characters WHERE age >= 100")
    
    
    #print the data frame containing the query results
    
    print("Centenarians:")
    centenarians.show()


    #stop spark context
    sc.stop()
