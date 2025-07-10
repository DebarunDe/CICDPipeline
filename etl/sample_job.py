import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContest
from pyspark.context import SparkContext

#for testing locally, wrap in try/except
#just for testing comm
try:
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    
    #sample data
    df = glueContext.spark_session.createDataFrame(
        [('Alice',34), ('Bob',45)], ['name','age']
    )
    
    df.show()
    
except Exception as e:
    print(f"ETL script failed: {e}")