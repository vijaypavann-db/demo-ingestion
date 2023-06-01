from databricks.connect import DatabricksSession
from databricks.sdk.core import Config

from com.db.fw.etl.core.pipeline.DeltaFromYaml import PrepareDelta
from com.db.fw.etl.core.common.Constants import *

if __name__ == "__main__":

    config = Config(profile = "DEV")
    spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()

    obj = PrepareDelta(spark, "./resources/1stload")
    obj.start()
    