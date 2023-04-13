import concurrent.futures
import logging, threading, time, multiprocessing

from com.db.fw.etl.core.pipeline.PipelineBuilder import *
from com.db.fw.etl.core.common.Constants import COMMON_CONSTANTS
from com.db.fw.etl.core.pipeline.PipelineUtils import PipelineUtils

class Runner:

    def __init__(self) -> None:
        self.metadata_db = COMMON_CONSTANTS.METADATA_DB
        self.entity_tbl = COMMON_CONSTANTS.ENTITY_TABLE
        self.entity_runs_tbl = COMMON_CONSTANTS.ENTITY_RUNS_TABLE
        self.spark = spark

    def run_pipeline(self, pipeline: Pipeline):
        # process_name = multiprocessing.current_process().name
        # print(f"{process_name}: running for {pipeline.name}")

        logging.info("Thread %s: starting - %s", pipeline.name, threading.get_ident())
        pipline_id, name, meta_jsons = PipelineBuilder.get_json_dump(pipeline)
        print("*******", pipline_id, name, meta_jsons)
        pipeline.start()

    def run(self):
        entity_runs = (self.spark.read.table(f"{self.metadata_db}.{self.entity_runs_tbl}") )
        
        entities = self.spark.read.table(f"{self.metadata_db}.{self.entity_tbl}")
        
        pipeline_details = (entities.join(entity_runs, entities["id"] == entity_runs["entity_id"], "inner")
                                    .select("run_id")
                                    .collect() )

        utils = PipelineUtils()
        run_ids = [utils.buildPipelineUsingRunId(row["run_id"]) for row in pipeline_details]
        print(run_ids)
        start_time = time.time()

        # TODO
        # with multiprocessing.Pool() as pool:
        #     pool.map(self.run_pipeline, run_ids)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            executor.map(self.run_pipeline, run_ids )

        print(f"Duration {time.time() - start_time} seconds")

if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")
    
    runner = Runner()
    runner.run()