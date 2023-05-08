from databricks.connect import DatabricksSession
from databricks.sdk.core import Config

from com.db.fw.etl.core.pipeline.PipelineBuilder import *
from com.db.fw.etl.core.pipeline.PipelineUtils import PipelineUtils


if __name__ == "__main__":

    config = Config(profile = "DEV")
    spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()

    utils = PipelineUtils(spark)
    run_id = "6b0ead19-c88f-4c16-a9cc-d11fbc27d16f"
    pipeline = utils.buildPipelineUsingRunId(run_id)
    pipline_id, name, meta_jsons = PipelineBuilder.get_json_dump(pipeline)
    print("*******", pipline_id, name, meta_jsons)
    pipeline.start()
