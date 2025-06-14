# How to Make Snowflake Work

This guide is for folks who have never used Snowflake or GitHub Actions before. It tells you how to set up your GitHub Actions secrets, create the Snowflake objects you require (such a warehouse, database, schema, and role), and send SQL code from this repository to your own Snowflake account.


## How to Install

1. Copy the repository

   ```bash 
    git clone https://github.com/yourusername/obd-telemetry-pipeline.git cd obd-telemetry-pipeline 
    ```
2. Set up and turn on a virtual environment

   ```bash 
    python -m venv venv 
    # macOS/Linux 
    source venv/bin/activate 
    # Windows 
    venv\Scripts\activate 
    ```
3. Set up the dependencies

   ```bash 
    pip install -r requirements.txt 
    ```

## Examples of how to use

```python 
from snowflake.snowpark import Session
from aobd_pipeline import main

# Configure your Snowflake connection
connection_parameters = {
    "account": "<account>",
    "user": "<user>",
    "password": "<password>",
    "role": "<role>",
    "warehouse": "<warehouse>",
    "database": "<database>",
    "schema": "<schema>"
}

session = Session.builder.configs(connection_parameters).create() result = main(session) print(result)
```

## How to Test

1. Make sure your virtual environment is running.
2. Run the tests on the unit:

   ```bash 
    pytest 
    ```
3. Use Black to check the formatting of your code:

   ```bash 
    black --check . 
    ```

## GitHub Template

You can use this repository as a starting point for your own Snowflake 
CI/CD pipeline:

1. Click "Use this template" on GitHub to make your own repo.
2. It will have all the code, procedures, and tests, but no login information.




## Putting it on Your Snowflake Account

To make sure that deployments only execute against *their* account, each user should set up their own Snowflake credentials as GitHub Actions secrets in *their* fork or template.

1. Go to **Settings → Secrets and variables → Actions** and add these secrets (with your own values):
   - `SNOWFLAKE_ACCOUNT`
   - `SNOWFLAKE_USER`
   - `SNOWFLAKE_ROLE`
   - `SNOWFLAKE_WAREHOUSE`
   - `SNOWFLAKE_DATABASE`
   - `SNOWFLAKE_SCHEMA`
   - `SNOWFLAKE_PRIVATE_KEY` (or PASSWORD) 
    - `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`

How do you discover them?
* `SNOWFLAKE_ACCOUNT`: This is how to find your account, like `xy12345.us-east-1`.
* When you log into Snowsight, look for it in the URL: `https://<account_locator>.snowflakecomputing.com/...`     
* Or run:

    ```sql 
    SELECT CURRENT_ACCOUNT(); 
    ```
     
* `SNOWFLAKE_USER`: The name of the Snowflake account you use to log in.

     * Run:

        ```sql 
        SELECT CURRENT_USER(); 
        ```
     
* `SNOWFLAKE_ROLE`: The role that queries execute under, like `SYSADMIN` or `DEVELOPER`.

     * Run:

        ```sql 
        SELECT CURRENT_ROLE(); 
        ```
     
    * To make one:

        ```sql 
        CREATE ROLE DEV_ROLE; 
        GRANT USAGE ON WAREHOUSE <your_wh> TO ROLE DEV_ROLE; 
        GRANT USAGE ON DATABASE <your_db> TO ROLE DEV_ROLE; 
        GRANT USAGE ON SCHEMA <your_schema> TO ROLE DEV_ROLE; 
        ```

* `SNOWFLAKE_WAREHOUSE`: The virtual warehouse for compute (e.g. `XS_WH`).

     * List what is already there:

       ```sql 
        SHOW WAREHOUSES; 
        ``` 
    * To make:

        ```sql 
        CREATE WAREHOUSE XS_WH 
        WAREHOUSE_SIZE = XSMALL 
        AUTO_SUSPEND = 1500 
        AUTO_RESUME = TRUE 
        INITIALLY_SUSPENDED = TRUE; 
        ```
* `SNOWFLAKE_DATABASE`: The name of the database, such "MY_DB.

    * List:

        ```sql 
        SHOW DATABASES; 
        ``` 

    * To create:

       ```sql 
        CREATE DATABASE MY_DB; 
        ``` 

* `SNOWFLAKE_SCHEMA`: The name of the schema (for example, "TEST_SCHEMA").

     * List:

       ```sql 
        SHOW SCHEMAS IN DATABASE MY_DB; 
        ``` 
        
    * To create:

       ```sql 
        CREATE SCHEMA TEST_SCHEMA; 
        ```
         
* `SNOWFLAKE_PRIVATE_KEY` *or* `PASSWORD`:

     * Use your login password for password authentication. To use key-pair authentication, make an RSA key on your own computer:

       ```bash 
        openssl genpkey -algorithm RSA -out snowflake_key.p8 -pkeyopt rsa_keygen_bits:2048 
        ```

       Then, if you like, you may encrypt it with a passphrase:

       ```bash 
        openssl pkcs8 -topk8 -in snowflake_key.p8 -out snowflake_key_enc.p8 -v2 aes-256-cbc 
        ```

* `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`:

     * The passphrase you used to encrypt your private key, if you did.

2. Start the workflow:

   * If you use "on: push," you can push to "main." 
   * If you use "workflow_dispatch," you can do it by hand by going to "Actions → Deploy to Snowflake → Run workflow."

---

Anyone can fork or template this repo, add their own secrets, and deploy it to their own Snowflake account or

- **Running in the Snowflake UI**

You can [open the `OBD_PROCEDURE.sql` file](src/worksheetCode/OBD_PROCEDURE.sql) to execute this directly in Snowflake's web interface, Snowsight. Copy the contents, paste them into a new worksheet, and run it.




