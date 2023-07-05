from databricks.connect import DatabricksSession
from databricks.sdk.core import Config

from com.db.fw.etl.core.pipeline.PipelineBuilder import *
from com.db.fw.etl.core.pipeline.PipelineUtils import PipelineUtils

if __name__ == "__main__":

    config = Config(profile = "DEV")
    spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()

    utils = PipelineUtils(spark)
    # run_id = "4cae5de8-5771-47de-bc1d-eae0daf84ab8"
    # pipeline = utils.buildPipelineUsingRunId(run_id)
    
    pipeline_id = "-164912534"
    pipeline = utils.buildPipeline(pipeline_id)

    # pipline_id, name, meta_jsons = PipelineBuilder.get_json_dump(pipeline)
    # print("*******", pipline_id, name, meta_jsons)
    # 923123834 run_id = "021fe8de-1436-11ee-81de-acde48001122"  lending_club_pipeline
    # -164912534 run_id = "7b849488-170e-11ee-9d47-acde48001122"  lending_club_pipeline_dq_checks
    pipeline.start(run_id = "7b849488-170e-11ee-9d47-acde48001122")
