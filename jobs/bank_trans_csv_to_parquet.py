import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import udf
from awsglue.dynamicframe import DynamicFrame
from dateutil import parser
from pyspark.sql.types import IntegerType,StringType,ArrayType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def parse_date_udf(x):
    #print("Input is: {}".format(x))
    #dt = parser.parse(x)
    return [x.year,x.month,x.day]

parse_date_udf = udf(parse_date_udf,ArrayType(IntegerType()))    
    
## @type: DataSource
## @args: [database = "banking", table_name = "raw_transactions", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
rawtranDyF = glueContext.create_dynamic_frame.from_catalog(database = "banking", table_name = "raw_transactions", transformation_ctx = "datasource0")

rawtranDyF.printSchema()


## @type: ApplyMapping
## @args: [mapping = [("tran_id", "long", "tran_id", "long"), ("acct_id", "long", "acct_id", "long"), ("tran_code", "string", "tran_code", "string"), ("tran_date", "string", "tran_date", "timestamp"), ("tran_amount", "double", "tran_amount", "double")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
mappedTranDyF = ApplyMapping.apply(frame = rawtranDyF, mappings = [("tran_id", "long", "tran_id", "long"), ("acct_id", "long", "acct_id", "long"), ("tran_code", "string", "tran_code", "string"), ("tran_date", "string", "tran_date", "timestamp"), ("tran_amount", "double", "tran_amount", "double")], transformation_ctx = "applymapping1")

## @type: ResolveChoice
## @args: [choice = "make_struct", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = mappedTranDyF, choice = "make_struct", transformation_ctx = "resolvechoice2")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")

tranDF = dropnullfields3.toDF()

tranDF = tranDF.withColumn('Year', parse_date_udf(tranDF["tran_date"])[0])\
    .withColumn('Month',parse_date_udf(tranDF["tran_date"])[1])

#tranDF = tranDF.orderBy('Year','Month')

tranDF = tranDF.repartition('Year','Month')

#print ("Number of partitions:{}".format(tranDF.rdd.getNumPartitions()))

tranDyF = DynamicFrame.fromDF(tranDF,glueContext,"tranDyF")

## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://<bucket/prefix>/transactions/"}, format = "parquet", transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_options(frame = tranDyF, connection_type = "s3", connection_options = {"path": "s3://<bucket/prefix>/transactions/", "partitionKeys": ["Year","Month"] }, format = "parquet", transformation_ctx = "datasink4")
job.commit()
