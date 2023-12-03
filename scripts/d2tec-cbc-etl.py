import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_null_rows
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame


# Script generated for node SplitLocalTransf
def SplitLocaliTransf(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, udf, when
    from pyspark.sql.types import StringType

    def clean_locali(locali):
        return locali.strip()

    def get_street(locali):
        return locali[:2]

    def get_build(locali):
        return locali[2:5]

    def get_level(locali):
        return locali[5:]

    cleanLocalilUDF = udf(lambda x: clean_locali(x), StringType())
    getStreetUDF = udf(lambda x: get_street(x), StringType())
    getBuildUDF = udf(lambda x: get_build(x), StringType())
    getLevelUDF = udf(lambda x: get_level(x), StringType())

    df = dfc.select(list(dfc.keys())[0]).toDF()
    df = df.withColumn("cbc_locali", cleanLocalilUDF(df.cbc_locali))
    df = df.withColumn(
        "cbc_locali", when(col("cbc_locali") == "", None).otherwise(col("cbc_locali"))
    )
    df = df.na.drop(subset=["cbc_locali"])
    df_new_data = df.withColumn("cbc_locali_street", getStreetUDF(df.cbc_locali))
    df_new_data = df_new_data.withColumn("cbc_locali_build", getBuildUDF(df.cbc_locali))
    df_new_data = df_new_data.withColumn("cbc_locali_level", getLevelUDF(df.cbc_locali))
    df_new_data = df_new_data.drop("cbc_locali")

    dyf_new_data = DynamicFrame.fromDF(df_new_data, glueContext, "cbc_locali")

    return DynamicFrameCollection({"CustomTransform0": dyf_new_data}, glueContext)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1701220378755 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://raw-data-d2tec/CBC030.csv"], "recurse": True},
    transformation_ctx="AmazonS3_node1701220378755",
)

# Script generated for node Change Schema
ChangeSchema_node1701220436246 = ApplyMapping.apply(
    frame=AmazonS3_node1701220378755,
    mappings=[
        ("cbc_filial", "string", "cbc_filial", "string"),
        ("cbc_codinv", "string", "cbc_codinv", "string"),
        ("cbc_num", "string", "cbc_num", "string"),
        ("cbc_cod", "string", "cbc_cod", "string"),
        ("cbc_local", "string", "cbc_local", "string"),
        ("cbc_quant", "string", "cbc_quant", "float"),
        ("cbc_qtdori", "string", "cbc_qtdori", "float"),
        ("cbc_locali", "string", "cbc_locali", "string"),
        ("cbc_lotect", "string", "cbc_lotect", "string"),
        ("cbc_numlot", "string", "cbc_numlot", "string"),
        ("cbc_numser", "string", "cbc_numser", "string"),
        ("cbc_codeti", "string", "cbc_codeti", "string"),
        ("cbc_contok", "string", "cbc_contok", "string"),
        ("cbc_ajust", "string", "cbc_ajust", "string"),
        ("cbc_idunit", "string", "cbc_idunit", "string"),
        ("cbc_coduni", "string", "cbc_coduni", "string"),
    ],
    transformation_ctx="ChangeSchema_node1701220436246",
)

# Script generated for node Remove Null Rows
RemoveNullRows_node1701220717534 = ChangeSchema_node1701220436246.gs_null_rows()

# Script generated for node SplitLocalTransf
SplitLocalTransf_node1701220801507 = SplitLocaliTransf(
    glueContext,
    DynamicFrameCollection(
        {"RemoveNullRows_node1701220717534": RemoveNullRows_node1701220717534},
        glueContext,
    ),
)

# Script generated for node SelColSplitLocalData
SelColSplitLocalData_node1701221166388 = SelectFromCollection.apply(
    dfc=SplitLocalTransf_node1701220801507,
    key=list(SplitLocalTransf_node1701220801507.keys())[0],
    transformation_ctx="SelColSplitLocalData_node1701221166388",
)

# Script generated for node Amazon S3
AmazonS3_node1701221325656 = glueContext.write_dynamic_frame.from_options(
    frame=SelColSplitLocalData_node1701221166388,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://transformed-data-d2tec/cbc/",
        "partitionKeys": [],
    },
    format_options={"compression": "uncompressed"},
    transformation_ctx="AmazonS3_node1701221325656",
)

job.commit()
