# demo-ingestion
Project to showcase / demo the capabilities of pyspark-ingestion framework  

---
## Project Setup
>### Step 1: How to clone the Repo
   
   1. Generate a `GitHub Classic PAT` Token (`Settings -> Developer settings -> Personal acces tokens -> Tokens (classic)`) using this <a href="https://github.com/settings/tokens" target="new">link</a>. <br/>
   Token looks like this `ghp_<ALPHA_NUMERIC_CHARS>`

   2. Clone the repo using "`git clone <REPO_URL>`"

      We can follow either of the below ways to clone ::
      
      i. Hardcode the Token:

      `git clone https://oauth2:ghp_<TOKEN>@github.com/vijaypavann-db/demo-ingestion.git`
     
      ii. Use the env variable: 
   
      `export TOKEN=ghp_<ALPHA_NUMERIC_CHARS>;
      git clone https://oauth2:$TOKEN@github.com/vijaypavann-db/demo-ingestion.git`

   3. Authenticate PAT Token (as described in the <a href="https://docs.github.com/en/enterprise-cloud@latest/authentication/authenticating-with-saml-single-sign-on/authorizing-a-personal-access-token-for-use-with-saml-single-sign-on" target="new">link</a>)      

>### Step 2: Open the project in VS Code. Install `python3.10` and create a Virtual Env [run  on Terminal]

   ```python
      python3.10 -m venv ./.denv
      source .denv/bin/activate
   ```
   
   Use [.denv-local] for local setup
   
   ```python
      python3.10 -m venv ./.denv-local
      source .denv-local/bin/activate
   ```
   * Use the same name for virtualenv `.denv` or `.denv-local` , as both are already added in `.gitignore` file.

#### Project Inital Setup [First Time Setup]
* Run `sh run-setup.sh` on Terminal
* It includes build creation 

* Use `sh local_setup/run-setup-local.sh` [**for Local Setup**]

#### Run Build [for new changes]
* Run `sh run-build.sh` on Terminal

* Use `sh local_setup/run-setup-local.sh` [**for Local Setup**]

##### * Verify the Wheel(.whl) file [in dist/] is of correct format as below: 
`{dist}-{version}(-{build})?-{python.version}-{os_platform}.whl`


>### Step 3: Run Code locally for development / testing
1. Install the module as below: 
`pip install dist/demo_ingestion-1.0-py3-none-any.whl  [--force-reinstall]`


--- 
## Run Pipeline

>### Step 1: Define Pipeline Metadata 
   1. Create a yaml file under `resources/pipeline_metadata/remote/<PIPELINE_NAME>` folder.
   2. Refer `1stload` / `rdms_load` for sample `Metadata`  

>### Step 2: Setup Database & Tables
   1. Run `demo/metadata_setup/db_tables/setup.py`. 
   [local-setup.py for local]
   This will setup the `database` and all the `tables` required.

>### Step 3: Run Pipeline Metadata Setup
   1. Open `demo/metadata_setup/pipeline/setup.py` 
   [local-setup.py for local]
   and change the `resource path` in `pipeline_metadata_path = "<RESOURCE_PATH>" ` to point to your `Metadata [yaml]` file. 
   2. It reads `Metadata [yaml]` files and inserts data into `pipeline_dependencies` and `pipeline_tasks` tables.

>### Step 4 : Run Pipeline
   1. Copy the `pipeline_id` from the `pipeline_dependencies` table and change it in `demo/pipelines/run/single-run.py`.
   2. `Run as Python file`.