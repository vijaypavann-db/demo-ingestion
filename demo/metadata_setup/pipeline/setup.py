from databricks.connect import DatabricksSession
from databricks.sdk.core import Config

from com.db.fw.etl.core.pipeline.DeltaFromYaml import PrepareDelta
from com.db.fw.etl.core.common.Constants import *

if __name__ == "__main__":

    config = Config(profile = "DEV")
    spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()

    # 1stload lending_club_load   lc_data_quality_load
    pipeline_metadata_path = "./resources/pipeline_metadata/remote/lc_data_quality_load"
    obj = PrepareDelta(spark, pipeline_metadata_path)
    obj.start()
    