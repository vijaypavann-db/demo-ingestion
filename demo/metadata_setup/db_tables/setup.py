from databricks.connect import DatabricksSession
from databricks.sdk.core import Config

from com.db.fw.metadata.setup import MetadataSetup

if __name__ == "__main__":

    config = Config(profile = "DEV")
    spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()

    # Add db_name = "<NEW_DB>", if you would like to change the default COMMON_CONSTANTS.METADATA_DB [demo_metadata]
    setup = MetadataSetup(spark, debug = True, dropTables = True)

    # Create all Tables
    setup.run()

    # Run for a single table
    # setup.createTable("pipeline_dependencies")
    # setup.createTable("pipeline_tasks")