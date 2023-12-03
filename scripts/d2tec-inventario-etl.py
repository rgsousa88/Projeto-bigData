import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_null_rows
from awsglue.dynamicframe import DynamicFrameCollection
import gs_to_timestamp
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs


# Script generated for node SplitLocaliData
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
    df = df.withColumn("cba_locali", cleanLocalilUDF(df.cba_locali))
    df = df.withColumn(
        "cba_locali", when(col("cba_locali") == "", None).otherwise(col("cba_locali"))
    )
    df = df.na.drop(subset=["cba_locali"])
    df_new_data = df.withColumn("cba_locali_street", getStreetUDF(df.cba_locali))
    df_new_data = df_new_data.withColumn("cba_locali_build", getBuildUDF(df.cba_locali))
    df_new_data = df_new_data.withColumn("cba_locali_level", getLevelUDF(df.cba_locali))
    df_new_data = df_new_data.drop("cba_locali")

    dyf_new_data = DynamicFrame.fromDF(df_new_data, glueContext, "cba_locali")

    return DynamicFrameCollection({"CustomTransform0": dyf_new_data}, glueContext)


# Script generated for node RemoveNullCBAData
def RemoveNullCBADataTransf(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, udf, when
    from pyspark.sql.types import StringType

    def to_null(strTime):
        if strTime == "        ":
            return ""

        return strTime

    emptyDataToNullUDF = udf(lambda x: to_null(x), StringType())

    df = dfc.select(list(dfc.keys())[0]).toDF()
    df_new_data = df.withColumn("cba_data", emptyDataToNullUDF(df.cba_data))
    df_new_data = df_new_data.withColumn(
        "cba_data", when(col("cba_data") == "", None).otherwise(col("cba_data"))
    )
    df_new_data = df_new_data.na.drop(subset=["cba_data"])
    dyf_new_data = DynamicFrame.fromDF(df_new_data, glueContext, "cba_data")

    return DynamicFrameCollection({"CustomTransform0": dyf_new_data}, glueContext)


# Script generated for node CBADataToTime
def CBADataToTimeTransf(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, udf
    from pyspark.sql.types import StringType

    def str_to_time(strTime):
        from datetime import datetime

        return datetime.strptime(strTime, "%Y%m%d").timestamp()

    strToTimeUDF = udf(lambda x: str_to_time(x), StringType())

    df = dfc.select(list(dfc.keys())[0]).toDF()
    df_new_data = df.withColumn("cba_data", strToTimeUDF(df.cba_data))
    dyf_new_data = DynamicFrame.fromDF(df_new_data, glueContext, "cba_data")

    return DynamicFrameCollection({"CustomTransform0": dyf_new_data}, glueContext)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1700485015916 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://raw-data-d2tec/CBA030 - INVENTARIO.csv"]},
    transformation_ctx="AmazonS3_node1700485015916",
)

# Script generated for node Change Schema
ChangeSchema_node1700485280572 = ApplyMapping.apply(
    frame=AmazonS3_node1700485015916,
    mappings=[
        ("cba_filial", "string", "cba_filial", "string"),
        ("cba_codinv", "string", "cba_codinv", "string"),
        ("cba_analis", "string", "cba_analis", "string"),
        ("cba_data", "string", "cba_data", "string"),
        ("cba_conts", "string", "cba_conts", "string"),
        ("cba_local", "string", "cba_local", "string"),
        ("cba_tipinv", "string", "cba_tipinv", "string"),
        ("cba_locali", "string", "cba_locali", "string"),
        ("cba_prod", "string", "cba_prod", "string"),
        ("cba_contr", "string", "cba_contr", "string"),
        ("cba_status", "string", "cba_status", "string"),
        ("cba_autrec", "string", "cba_autrec", "string"),
        ("cba_classa", "string", "cba_classa", "string"),
        ("cba_classb", "string", "cba_classb", "string"),
        ("cba_classc", "string", "cba_classc", "string"),
        ("cba_xqtdcf", "string", "cba_xqtdcf", "string"),
    ],
    transformation_ctx="ChangeSchema_node1700485280572",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1700491153395 = DynamicFrame.fromDF(
    ChangeSchema_node1700485280572.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1700491153395",
)

# Script generated for node Remove Null Rows
RemoveNullRows_node1700491557374 = DropDuplicates_node1700491153395.gs_null_rows()

# Script generated for node SplitLocaliData
SplitLocaliData_node1701139696177 = SplitLocaliTransf(
    glueContext,
    DynamicFrameCollection(
        {"RemoveNullRows_node1700491557374": RemoveNullRows_node1700491557374},
        glueContext,
    ),
)

# Script generated for node SelColSplitLocaliData
SelColSplitLocaliData_node1701141137636 = SelectFromCollection.apply(
    dfc=SplitLocaliData_node1701139696177,
    key=list(SplitLocaliData_node1701139696177.keys())[0],
    transformation_ctx="SelColSplitLocaliData_node1701141137636",
)

# Script generated for node RemoveNullCBAData
RemoveNullCBAData_node1701131600124 = RemoveNullCBADataTransf(
    glueContext,
    DynamicFrameCollection(
        {
            "SelColSplitLocaliData_node1701141137636": SelColSplitLocaliData_node1701141137636
        },
        glueContext,
    ),
)

# Script generated for node SelCollNullCBAData
SelCollNullCBAData_node1701132529807 = SelectFromCollection.apply(
    dfc=RemoveNullCBAData_node1701131600124,
    key=list(RemoveNullCBAData_node1701131600124.keys())[0],
    transformation_ctx="SelCollNullCBAData_node1701132529807",
)

# Script generated for node CBADataToTime
CBADataToTime_node1700510420056 = CBADataToTimeTransf(
    glueContext,
    DynamicFrameCollection(
        {"SelCollNullCBAData_node1701132529807": SelCollNullCBAData_node1701132529807},
        glueContext,
    ),
)

# Script generated for node SelCollToTime
SelCollToTime_node1700515221102 = SelectFromCollection.apply(
    dfc=CBADataToTime_node1700510420056,
    key=list(CBADataToTime_node1700510420056.keys())[0],
    transformation_ctx="SelCollToTime_node1700515221102",
)

# Script generated for node To Timestamp
ToTimestamp_node1700514346233 = SelCollToTime_node1700515221102.gs_to_timestamp(
    colName="cba_data", colType="autodetect"
)

# Script generated for node Amazon S3
AmazonS3_node1700509168290 = glueContext.write_dynamic_frame.from_options(
    frame=ToTimestamp_node1700514346233,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://transformed-data-d2tec/inventario/",
        "partitionKeys": [],
    },
    format_options={"compression": "uncompressed"},
    transformation_ctx="AmazonS3_node1700509168290",
)

job.commit()
