import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs


# Script generated for node RemoveDeletedFieldTransf
def RemoveDeletedFieldTransf(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, udf, when

    df = dfc.select(list(dfc.keys())[0]).toDF()
    df = df.withColumn(
        "d_e_l_e_t_", when(col("d_e_l_e_t_") == "*", None).otherwise(col("d_e_l_e_t_"))
    )
    df = df.na.drop(subset=["d_e_l_e_t_"])
    df_new_data = df.drop("d_e_l_e_t_")

    dyf_new_data = DynamicFrame.fromDF(df_new_data, glueContext, "usr_delet")
    return DynamicFrameCollection({"CustomTransform0": dyf_new_data}, glueContext)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1701215261959 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://raw-data-d2tec/Usuarios.csv"], "recurse": True},
    transformation_ctx="AmazonS3_node1701215261959",
)

# Script generated for node Change Schema
ChangeSchema_node1701215638194 = ApplyMapping.apply(
    frame=AmazonS3_node1701215261959,
    mappings=[
        ("usr_id", "string", "usr_id", "string"),
        ("usr_uuid", "string", "usr_uuid", "string"),
        ("usr_codigo", "string", "usr_codigo", "string"),
        ("usr_nome", "string", "usr_nome", "string"),
        ("usr_msblql", "string", "usr_msblql", "string"),
        ("usr_msblqd", "string", "usr_msblqd", "string"),
        ("usr_email", "string", "usr_email", "string"),
        ("usr_depto", "string", "usr_depto", "string"),
        ("usr_cargo", "string", "usr_cargo", "string"),
        ("usr_ano", "string", "usr_ano", "string"),
        ("usr_versao", "string", "usr_versao", "string"),
        ("usr_chgpsw", "string", "usr_chgpsw", "string"),
        ("usr_idmid", "string", "usr_idmid", "string"),
        ("usr_codsal", "string", "usr_codsal", "string"),
        ("usr_dtinc", "string", "usr_dtinc", "string"),
        ("usr_key_sp", "string", "usr_key_sp", "string"),
        ("usr_seq_sp", "string", "usr_seq_sp", "string"),
        ("usr_dtbase", "string", "usr_dtbase", "string"),
        ("usr_redtbs", "string", "usr_redtbs", "string"),
        ("usr_avdtbs", "string", "usr_avdtbs", "string"),
        ("usr_allemp", "string", "usr_allemp", "string"),
        ("d_e_l_e_t_", "string", "d_e_l_e_t_", "string"),
        ("usr_serie_sp", "string", "usr_serie_sp", "string"),
        ("usr_qtdexppsw", "string", "usr_qtdexppsw", "string"),
        ("usr_grprule", "string", "usr_grprule", "string"),
        ("usr_needrole", "string", "usr_needrole", "string"),
        ("usr_dtchgpsw", "string", "usr_dtchgpsw", "string"),
        ("usr_dtavichgpsw", "string", "usr_dtavichgpsw", "string"),
        ("usr_dtlogon", "string", "usr_dtlogon", "string"),
        ("usr_hrlogon", "string", "usr_hrlogon", "string"),
        ("usr_iplogon", "string", "usr_iplogon", "string"),
        ("usr_cnlogon", "string", "usr_cnlogon", "string"),
        ("usr_usersologon", "string", "usr_usersologon", "string"),
        ("usr_dttentblq", "string", "usr_dttentblq", "string"),
        ("usr_hrtentblq", "string", "usr_hrtentblq", "string"),
        ("usr_qtdtentblq", "string", "usr_qtdtentblq", "string"),
        ("usr_datablq", "string", "usr_datablq", "string"),
        ("usr_horablq", "string", "usr_horablq", "string"),
        ("usr_l_admin_ch", "string", "usr_l_admin_ch", "string"),
        ("usr_blq_usr", "string", "usr_blq_usr", "string"),
        ("usr_dtalastalt", "string", "usr_dtalastalt", "string"),
        ("usr_hrlastalt", "string", "usr_hrlastalt", "string"),
        ("usr_typeblock", "string", "usr_typeblock", "string"),
        ("usr_qtdacessos", "string", "usr_qtdacessos", "string"),
        ("usr_timeout", "string", "usr_timeout", "string"),
        ("usr_listner", "string", "usr_listner", "string"),
        ("usr_nivelread", "string", "usr_nivelread", "string"),
    ],
    transformation_ctx="ChangeSchema_node1701215638194",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1701216564323 = DynamicFrame.fromDF(
    ChangeSchema_node1701215638194.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1701216564323",
)

# Script generated for node RemoveDeletedFieldTransf
RemoveDeletedFieldTransf_node1701217060925 = RemoveDeletedFieldTransf(
    glueContext,
    DynamicFrameCollection(
        {"DropDuplicates_node1701216564323": DropDuplicates_node1701216564323},
        glueContext,
    ),
)

# Script generated for node SelColDeleted
SelColDeleted_node1701217726349 = SelectFromCollection.apply(
    dfc=RemoveDeletedFieldTransf_node1701217060925,
    key=list(RemoveDeletedFieldTransf_node1701217060925.keys())[0],
    transformation_ctx="SelColDeleted_node1701217726349",
)

# Script generated for node Amazon S3
AmazonS3_node1701218081726 = glueContext.getSink(
    path="s3://transformed-data-d2tec",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1701218081726",
)
AmazonS3_node1701218081726.setCatalogInfo(
    catalogDatabase="d2tec-inv-db", catalogTableName="usuario_cleaned"
)
AmazonS3_node1701218081726.setFormat("glueparquet")
AmazonS3_node1701218081726.writeFrame(SelColDeleted_node1701217726349)
job.commit()
