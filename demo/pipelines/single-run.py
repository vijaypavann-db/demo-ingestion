from databricks.connect import DatabricksSession
from databricks.sdk.core import Config

from com.db.fw.etl.core.pipeline.PipelineBuilder import *
# from com.db.fw.etl.core.pipeline.PipelineUtils import PipelineUtils
from demo.pipelines.PipelineUtilsV1 import PipelineUtils


if __name__ == "__main__":

    config = Config(profile = "DEV")
    spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()

    utils = PipelineUtils(spark)
    # run_id = "4cae5de8-5771-47de-bc1d-eae0daf84ab8"
    # pipeline = utils.buildPipelineUsingRunId(run_id)
    
    pipeline_id = "1964147365"
    pipeline = utils.buildPipeline(pipeline_id)

    # pipline_id, name, meta_jsons = PipelineBuilder.get_json_dump(pipeline)
    # print("*******", pipline_id, name, meta_jsons)
    pipeline.start() # run_id = "a17991ee-fe03-11ed-8182-aa665a13f2b0"
