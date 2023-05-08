from databricks.sdk import WorkspaceClient
import base64

# w = WorkspaceClient(profile = "DEV")

w = WorkspaceClient(host = "https://adb-984752964297111.11.azuredatabricks.net",
                    token = "dapi69d3d035e76d456628ae5d0fea721859"
                   )

file_path  = "/zzz_hello.txt"
file_data  = "Hello, Databricks! Pavan done this"

# The data must be base64-encoded before being written.
file_data_base64 = base64.b64encode(file_data.encode())

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