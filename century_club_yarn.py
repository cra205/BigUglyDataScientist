from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext


if __name__ == "__main__":

    sparkConf = SparkConf().setMaster("local").setAppName("MongoSparkConnectorTour").set("spark.app.id", "MongoSparkConnectorTour")
    sc = SparkContext(conf=sparkConf)
    sqlContext = SQLContext(sc)

    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.FATAL)

    # Save some data
    charactersRdd = sc.parallelize([("Bilbo Baggins",  50), ("Gandalf", 1000), ("Thorin", 195), ("Balin", 178), ("Kili", 77),
                                    ("Dwalin", 169), ("Oin", 167), ("Gloin", 158), ("Fili", 82), ("Bombur", None)])
    characters = sqlContext.createDataFrame(charactersRdd, ["name", "age"])
    
    # the original write config was as follows:
    #characters.write.format("com.mongodb.spark.sql").mode("overwrite").save()
    
    characters.write.format("com.mongodb.spark.sql.DefaultSource").option("spark.mongodb.output.uri","mongodb://dylan:abrams@apsrd6777.uhc.com:27017/admin").option("spark.mongodb.output.database","sandbox").option("spark.mongodb.output.collection","pythontest").mode("overwrite").save()


    # Load the data
    #the original read config was as follows:
    #df = sqlContext.read.format("com.mongodb.spark.sql").load()
    
    df = sqlContext.read.format("com.mongodb.spark.sql.DefaultSource").option("spark.mongodb.input.uri","mongodb://dylan:abrams@apsrd6777.uhc.com:27017/admin").option("spark.mongodb.input.database","sandbox").option("spark.mongodb.input.collection","pythontest").load()
    
    print("Schema:")
    df.printSchema()

    # SQL
    df.registerTempTable("characters")
    centenarians = sqlContext.sql("SELECT name, age FROM characters WHERE age >= 100")
    print("Centenarians:")
    centenarians.show()

    sc.stop()
