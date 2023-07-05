from databricks.sdk import WorkspaceClient
import base64

w = WorkspaceClient(profile = "DEV")

""" Write the CSV data into DB Workspace using `Databricks SDK`
"""

dir_path = "/tmp"
file_path  = f"{dir_path}/inp.csv"
file_data  =  """id|name|grade
0|VJ|A
1|PV|B
2|NV|C
3|VN|D
4|P1|E
5|NNP|F
""".strip()

# The data must be base64-encoded before being written.
file_data_base64 = base64.b64encode(file_data.encode())

w.dbfs.mkdirs(dir_path)

# Create the file.
file_handle = w.dbfs.create(
  path      = file_path,
  overwrite = True
).handle

# Add the base64-encoded version of the data.
w.dbfs.add_block(
  handle = file_handle,
  data   = file_data_base64.decode()
)

# Close the file after writing.
w.dbfs.close(handle = file_handle)

# Read the file's contents and then decode and print it.
response = w.dbfs.read(path = file_path)
print(base64.b64decode(response.data).decode())

# Delete the file.
# w.dbfs.delete(path = file_path)