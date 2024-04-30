# FAQ (P8 BigQuery)

## Reauthentication is needed

`gcloud auth application` times out after 12 hours. So, this is a common scenario. Run the command again

```
gcloud auth application-default login --scopes=openid,https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/drive.readonly
```


## Model not found

Errors like `NotFound: 404 Not found: Model <project>:<dataset>.<model>` happens if you are refering to a model that has not been created yet. This is possible in two cases:

(1) You have not actually ran a query to create a model. Fix: run the query.

(2) You have ran a query, which has not been finished. 

Fix: force your code to wait until the query is finished. This happens automatically, when you use `%%bigquery` magic. 

Or, if you are using `bg.query` then add `.to_dataframe()` to force your code to wait for the query result to come back.

```
q = bq.query("""
    ... your model creation query ...
""")
q.to_dataframe()
```
