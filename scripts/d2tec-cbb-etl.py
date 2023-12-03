import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_null_rows

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1701219548788 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://raw-data-d2tec/CBB030.csv"], "recurse": True},
    transformation_ctx="AmazonS3_node1701219548788",
)

# Script generated for node Change Schema
ChangeSchema_node1701219589287 = ApplyMapping.apply(
    frame=AmazonS3_node1701219548788,
    mappings=[
        ("cbb_filial", "string", "cbb_filial", "string"),
        ("cbb_num", "string", "cbb_num", "string"),
        ("cbb_codinv", "string", "cbb_codinv", "string"),
        ("cbb_usu", "string", "cbb_usu", "string"),
        ("cbb_ncont", "string", "cbb_ncont", "string"),
        ("cbb_status", "string", "cbb_status", "string"),
    ],
    transformation_ctx="ChangeSchema_node1701219589287",
)

# Script generated for node Remove Null Rows
RemoveNullRows_node1701219725590 = ChangeSchema_node1701219589287.gs_null_rows()

# Script generated for node Amazon S3
AmazonS3_node1701219833371 = glueContext.write_dynamic_frame.from_options(
    frame=RemoveNullRows_node1701219725590,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://transformed-data-d2tec/cbb/",
        "partitionKeys": [],
    },
    format_options={"compression": "uncompressed"},
    transformation_ctx="AmazonS3_node1701219833371",
)

job.commit()
