# demo-ingestion
Project to showcase/ demo the capabilities of pyspark-ingestion framework  

---
### * Clone the Repository
1. Generate a (Classic) PAT Token, e.g. ghp_AU43sdfsdfsdfsdf

2. Clone the repo using "git clone <URL>"

   i. Hardcode the Token:

      `git clone https://oauth2:ghp_<TOKEN>@github.com/vijaypavann-db/demo-ingestion.git`
     
   ii. Use the env variable: 
   
      `export TOKEN=ghp_AU43sdfsdfsdfsdf;
      git clone https://oauth2:$TOKEN@github.com/vijaypavann-db/demo-ingestion.git`

### * Create a Virtual Env [run  on Terminal]
```python
pip3 install virtualenv
virtualenv denv
source denv/bin/activate
```
#### Project Inital Setup
* Run `sh run-setup.sh` on Terminal
* It includes build creation 

#### Run Build [for new changes]
* Run `sh run-build.sh` on Terminal

##### * Builds the Wheel(.whl) distribution
1. Installs the `wheel` & `check-wheel-contents` modules
 ```python
    pip install wheel
    pip install check-wheel-contents
    pip install dependencies/pyspark_ingestion-1.0-py3-none-any.whl  --upgrade --target .
 ```
2. Runs the setup
```python
python setup.py bdist_wheel 
check-wheel-contents dist
```

* Verify the Wheel(.whl) file [in dist/] is of correct format as below: 
`{dist}-{version}(-{build})?-{python.version}-{os_platform}.whl`