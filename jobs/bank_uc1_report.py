import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
import logging

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','s3Outputlocation'])

MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger('uc1-report-logger')

logger.setLevel(logging.INFO)

logger.info("Initializing Glue Job")

sc=sc if 'sc' in vars() else SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
s3Outputlocation = args['s3Outputlocation']

tranDyF = glueContext.create_dynamic_frame.from_catalog(database='banking',table_name='transactions')
acctDyF = glueContext.create_dynamic_frame.from_catalog(database='banking',table_name='accounts')
custDyF = glueContext.create_dynamic_frame.from_catalog(database='banking',table_name='customers')

logger.info("Data Read from Catalog")


tranDyF.toDF().createTempView("tran_view")

logger.info("Created Temp Table tran_view")


tranfilterdDF = spark.sql("SELECT acct_id,month\
    , CAST(sum((CASE tran_code WHEN \'S\' THEN tran_amount ELSE 0 END)) AS decimal(10,2)) as service_charges\
    , CAST(sum((CASE tran_code WHEN \'D\' THEN tran_amount ELSE 0 END)) AS decimal(10,2)) as debit_charges\
    , CAST(sum((CASE tran_code WHEN \'C\' THEN tran_amount ELSE 0 END)) AS decimal(10,2)) as credit_charges\
    , CAST(sum((CASE tran_code WHEN \'L\' THEN tran_amount ELSE 0 END)) AS decimal(10,2)) as late_fees\
    FROM\
    tran_view\
    GROUP BY acct_id, month")
    
tranfilteredDyF = DynamicFrame.fromDF(tranfilterdDF,glueContext,name="tranfilteredDyF")
uc1JoinDyF = Join.apply(frame1= Join.apply(tranfilteredDyF,acctDyF,"acct_id","account_id",transformation_ctx='join1')\
    ,frame2=custDyF,keys1="customer_id",keys2="customer_id", transformation_ctx="join2" )\
    .select_fields(["first_name","last_name","account_name","month","service_charges","debit_charges","credit_charges","late_fees"])
    
uc1JoinDF = uc1JoinDyF.toDF()

logger.info("Joined Query executed successfully")

uc1DF = uc1JoinDF.select(concat(uc1JoinDF['first_name'],lit(' '),uc1JoinDF['last_name']).alias('Customer Name'),\
                        "account_name", "month","service_charges","debit_charges","credit_charges","late_fees")
                        
logger.info("Concatenated Customer name column")
                        
uc1DF1 = uc1DF.repartition(100) 

logger.info("Partitioned the output")


uc1DyF = DynamicFrame.fromDF(uc1DF1,glueContext,"uc1DyF")

uc1DyF = uc1DyF.rename_field("account_name","Account Number")\
    .rename_field("month","Transaction Month")\
    .rename_field("service_charges","Sum of Service Charges")\
    .rename_field("debit_charges","Sum of Debit Charges")\
    .rename_field("credit_charges","Sum of Credit Charges")\
    .rename_field("late_fees","Sum Of Late_fees")
    
logger.info("Renamed Column names")
    
    
#glueContext.write_dynamic_frame.from_options(frame = uc1DyF, connection_type = "s3", connection_options={ "path": s3Outputlocation, "compressionType":"gzip"}, format_options={"separator":",","withHeader":True,"writeHeader":True}, format="csv")
glueContext.write_dynamic_frame.from_options(frame = uc1DyF, connection_type = "s3", connection_options={ "path": s3Outputlocation }, format="parquet")

logger.info("Successfull write to S3")


job.commit()
    
