# demo-ingestion
Project to showcase/ demo the capabilities of pyspark-ingestion framework  

---
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

   * Use the same name for virtualenv `.denv` [python_virtualenv], as it's already added in `.gitignore` file.

#### Project Inital Setup [First Time Setup]
* Run `sh run-setup.sh` on Terminal
* It includes build creation 

#### Run Build [for new changes]
* Run `sh run-build.sh` on Terminal

##### * Verify the Wheel(.whl) file [in dist/] is of correct format as below: 
`{dist}-{version}(-{build})?-{python.version}-{os_platform}.whl`


>### Step 3: Run Code locally for development / testing
1. Install the module as below: 
`pip install dist/demo_ingestion-1.0-py3-none-any.whl  [--force-reinstall]`