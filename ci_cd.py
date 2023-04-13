from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
import shutil
import os

# Remove build dirs if exist!
shutil.rmtree("dist/", ignore_errors = True)
shutil.rmtree("build/", ignore_errors = True)

print(f"curr_dir: {os.path.curdir}")

# Build wheel distribution [locally]
print("Building Wheel file (1)")
print(os.system("python setup.py bdist_wheel"))

path_exists = os.path.exists(f"dist/demo_ingestion-1.0-py3-none-any.whl")
print(path_exists)

print("Connecting to Databricks Environment Step (2)")
spark = SparkSession.builder.getOrCreate()

print("Moving Wheel file to designated DBFS location  (3)")
dbutils = DBUtils(spark)
output= dbutils.fs.cp('file:dist/demo_ingestion-1.0-py3-none-any.whl', 'dbfs:/tmp/demo_ingestion-1.0-py3-none-any')

print(output)
display(output)

path_exists = os.path.exists(f"dist/demo_ingestion-1.0-py3-none-any.whl")
print(path_exists)

print("Remove local build dirs (4)")
shutil.rmtree("build/", ignore_errors=True)
shutil.rmtree("dist", ignore_errors = True)



