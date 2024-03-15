# DRAFT!  Don't start yet.

# P5 (6% of grade): Spark, Loan Applications

## :telescope: Overview

In P5, we'll use Spark to analyze loan applications in WI.  You'll
load your data to Hive tables and views so you can easily query them.
The big table (loans) has many IDs in columns; you'll need to join
these against other tables or views to determine the meaning of these
IDs.  In addition, you'll practice training a Decision Tree model to
predict loan approval.

**Important:** You'll answer ten questions in P5.  Write
  each question and it's number (e.g., "#q1: ...") as a comment in your
  notebook before each answer so we can easily search your notebook
  and give you credit for your answers.

Learning objectives:

* use Spark's RDD, DataFrame, and SQL interfaces to answer questions about data
* load data into Hive for querying with Spark
* optimize queries with bucketing and caching
* train a Random Forest model

Before starting, please revisit the [general project directions](../projects.md).

## :pushpin: Corrections/Clarifications
None


## :hammer_and_wrench: Cluster Setup

### Development Environment

Click the GitHub classroom link and initiate your repository for this project. Now, clone the project repository on your VM. Your clone repository should have a `setup.sh` file inside. Run `./setup.sh` to download all the relevant files for this project. Note that, you may have to run the `setup.sh` again to update the files if there are any changes.

### Virtual Machine

#### Cleanup Docker

Due to storage constraints in your VM, you should remove any previous docker images, containers and networks. To stop any running containers, run `docker stop $(docker ps -aq)`. Then, you should run `docker system prune -af` to remove all the stopped containers, related docker images and networks. 

You may repeat these steps to clean up the storage if your disk becomes full.

#### Increase Swap Space

~4 GB is barely enough for P5. Before you start, take a moment to enable a 1
GB swap file to supplement.  A swap file is on storage but acts
as extra memory. This has performance implications as storage is
much slower than RAM (as we have studied in class).  

Run the following commands in your Google Cloud VM to increase the size of the swap to **1 GB**. You can optionally go through this [blog](https://www.digitalocean.com/community/tutorials/how-to-add-swap-space-on-ubuntu-22-04) to know the details.
```
sudo fallocate -l 1G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

#### Check Swap Space

Now, you should check if the swap space has actually been increased. Run the command `free -m`. This will show the total size of Swap in megabytes.

### Containers

For this project, you'll deploy a small cluster of containers. Take a look at the table below to understand the docker images we need and how many containers will be created. You should read the [`docker-compose.yml`](./docker-compose.yml) file to understand better.

| Image Name     | Purpose        | Container Count | Dockerfile            |
| -------------- | -------------- | --------------- | --------------------- |
| `p5-base`      | Base Image     | 0               | `p5-base.Dockerfile`  |
| `p5-nb`        | Jupyter        | 1               | `notebook.Dockerfile` |
| `p5-nn`        | NameNode       | 1               | `namenode.Dockerfile` |
| `p5-dn`        | DataNode       | 1               | `datanode.Dockerfile` |
| `p5-boss`      | Spark boss     | 1               | Create yourself       |
| `p5-worker`    | Spark worker   | 2               | Create yourself       |

#### Boss and Worker Dockerfile

We only provide four of the dockerfiles. You'll need to write 
`boss.Dockerfile` and `worker.Dockerfile` yourself. These Dockerfiles will invoke
the Spark boss and workers and will use `p5-base` as their base Docker image.

To start the Spark boss and workers, you will need to run the `start-master.sh` 
and `start-worker.sh
spark://boss:7077 -c 1 -m 512M` commands respectively (you'll need to specify
the full path to these .sh scripts). These scripts launch the Spark
boss and workers in the background and then exit. Make sure that the containers
do not exit along with the script and instead keep running until manually stopped.

#### Build Images

Once all of the six dockerfiles are ready, you should be able to build them all using the following commands.

```bash
docker build . -f p5-base.Dockerfile -t p5-base
docker build . -f notebook.Dockerfile -t p5-nb
docker build . -f namenode.Dockerfile -t p5-nn
docker build . -f datanode.Dockerfile -t p5-dn
docker build . -f boss.Dockerfile -t p5-boss
docker build . -f worker.Dockerfile -t p5-worker
```

Be patient, as it takes a while to build all of these. To check, run `docker images`. You should be able to see the names of these six images.

#### Start the Cluster

You should then be able to use the `docker-compose.yml` we provided to
run `docker compose up -d`.  Wait a bit and make sure all containers
are still running. You can run `docker ps` to check if there are **six** containers running.

If some are starting up and then exiting, troubleshoot
the reason before proceeding further.

## :dvd: Data Setup

<!-- ### Virtual Machine

The Docker Compose setup maps a	`nb` directory into your Jupyter
container.  Within `nb`, you need to create a subdirectory called
`data` and fill it with some CSVs you'll use for the project.

You can	run the	following on your VM (not in any container):

```
wget https://pages.cs.wisc.edu/~harter/cs544/data/hdma-wi-2021.zip
wget https://pages.cs.wisc.edu/~harter/cs544/data/arid2017_to_lei_xref_csv.zip
wget https://pages.cs.wisc.edu/~harter/cs544/data/code_sheets.zip
mkdir -p nb/data
unzip -o hdma-wi-2021.zip -d nb/data
unzip -o arid2017_to_lei_xref_csv.zip -d nb/data
unzip -o code_sheets.zip -d nb/data
```

You'll probably	need to	change some permissions	(`chmod`) or run as
root (`sudo su`) to be able to do this. -->

### Jupyter Container

Connect to JupyterLab inside your container. Take a look inside the `/nb` folder.
You should be able to see a `p5.ipynb` file and a `data` folder. The structure of the `nb` folder is as follows.

```
/
|--- nb
     |--- p5.ipynb
     |--- data
          |--- action_taken.csv
          |--- agency.csv
          |--- <other csv files>
          |--- tracts.csv
```

Run the following shell command in a notebook cell to upload the CSVs from the local file system to HDFS.

```
hdfs dfs -D dfs.replication=1 -cp -f data/*.csv hdfs://nn:9000/
```

## :bank: Part 1: Filtering: RDDs, DataFrames, and Spark

Inside your `p5.ipynb` notebook, create a Spark session (note we're enabling
Hive on HDFS):

```python
from pyspark.sql import SparkSession
spark = (SparkSession.builder.appName("cs544")
         .master("spark://boss:7077")
         .config("spark.executor.memory", "512M")
         .config("spark.sql.warehouse.dir", "hdfs://nn:9000/user/hive/warehouse")
         .enableHiveSupport()
         .getOrCreate())
```

#### Q1: How many banks contain the words "The" and "National" (case sensitive) in their name? Use an **RDD** to answer.

The `arid2017_to_lei_xref_csv.csv` contains the banks, so you can use the following to read it to a DataFrame.

```python
# TODO: modify to treat the first row as a header
# TODO: modify to infer the schema
banks_df = spark.read.csv("hdfs://nn:9000/arid2017_to_lei_xref_csv.csv")
```

From this DataFrame, you can use `banks_df.rdd` to get the underlying
RDD you need for this question.

Use a `filter` transformation (that takes a Python lambda) with a
`count` action to get the answer.

As a practice, you could run this:

```python
rows = df.rdd.take(3)
```

They try to extract the name from one of the rows with a little Python
code.  This will help you determine how to write your lambda (which
will take a `Row` and return a boolean).

Use the same format as previous projects for all your notebook answers
(the last line of a cell contains an expression giving the answer, and the
the cell starts with a "#q1" comment, or similar).

#### Q2: Same as Q1. This time, use a **DataFrame** (instead of RDD) to answer.

This is the same as Q1, but now you must operate on `banks_df` itself,
without directly accessing the underlying `RDD`.

DataFrames also have `filter` transformations and `count` actions, but
`filter` takes a string containing a condition.  The condition uses
the same syntax as a condition in SQL, so these resources may help:

* https://www.w3schools.com/sql/sql_like.asp
<!-- * https://www.w3schools.com/sql/func_sqlserver_lower.asp -->

#### Q3: Same as Q1. This time, use **Spark SQL** (instead of RDD) to answer.

To write a SQL query to answer this, we first need to load it into a Hive
table.  You can do so with this:

```python
banks_df.write.saveAsTable("banks", mode="overwrite")
```

Now you can use `spark.sql(????)` with a SQL query you write to get
the answer.  This call will return the results as a Spark DataFrame --
you'll need to do a little extra Python work to get this out as a
single int for your answer.

Answers to Q1, Q2 and Q3 should be the same.

## :post_office: Part 2: Hive Data Warehouse

#### `loans` table

You have already added a `banks` table to Hive (using the command we shared with you). Now, write similar code `hdma-wi-2021.csv` into a table called `loans`. 

Use
[`bucketBy`](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrameWriter.bucketBy.html)
with your `saveAsTable` call to create 8 buckets on column
`county_code`.  This means that your written data will be broken into
8 buckets/groups, and all the rows will the same county will be in the
same bucket/group.  This will make some queries faster (for example,
if you `GROUP BY` on `county_code`, Spark might be able to avoid
shuffling/exchanging data across partitions/machines).

**Extra:** Note that, `saveAsTable` will produce one or more Parquet files in
`hdfs://nn:9000/user/hive/warehouse`. Look back at how you created
your Spark session to see this. You can run `!hdfs dfs -ls hdfs://nn:9000/user/hive/warehouse/loans` to see the parquet files of `loans` table.

#### Other views

Use `createOrReplaceTempView` to create Hive views for each of the names in this list:

```python
["ethnicity", "race", "sex", "states", "counties", "tracts", "action_taken",
 "denial_reason", "loan_type", "loan_purpose", "preapproval", "property_type"]
```

The contents should correspond to the CSV files of the same name in
HDFS.  Don't forget about headers and schema inference!

#### Q4: What tables are in our warehouse?

You can use `spark.sql("SHOW TABLES").show()` to see your tables in the warehouse as follows. 

```
+---------+-------------+-----------+
|namespace|    tableName|isTemporary|
+---------+-------------+-----------+
|  default|        banks|      false|
|  default|        loans|      false|
|         | action_taken|       true|
|         |     counties|       true|
|         |denial_reason|       true|
|         |    ethnicity|       true|
|         | loan_purpose|       true|
|         |    loan_type|       true|
|         |  preapproval|       true|
|         |property_type|       true|
|         |         race|       true|
|         |          sex|       true|
|         |       states|       true|
|         |       tracts|       true|
+---------+-------------+-----------+
```
Answer with a Python dict looks like this
```python
{'banks': False, 
 'loans': False, 
 'action_taken': True, 
 'counties': True, 
 'denial_reason': True, 
 'ethnicity': True, 
 'loan_purpose': True, 
 'loan_type': True, 
 'preapproval': True, 
 'property_type': True, 
 'race': True, 
 'sex': True, 
 'states': True, 
 'tracts': True
 }
```


#### Q5: How many loan applications has the bank "First National Bank" received in 2020 in this dataset?

Use an `INNER JOIN` between `banks` (`banks.lei_2020`) and `loans` (`loans.lei`) to answer this
question.  `lei` in `loans` lets you identify the bank.  Filter on
`respondent_name`. Do NOT hardcode the `lei_2020`.

#### Q6: What does `.explain("formatted")` tell us about how Spark executes Q5?

Show the output, then write comments (which we will manually grade) explaining the following:

1. Which table is sent to every executor via a `BroadcastExchange` operation?
2. Does the plan involve `HashAggregate`s (depending on how you write the query, it may or may not)?  If so, which ones?

For this, have a cell in your notebook that looks like the following:

```python
#q6
# .. your ..
# .. answers ..
```

## :basket: Part 3: Grouping Rows

#### Q7: What are the application counts for Wells Fargo applications for the ten counties where Wells Fargo applications have the highest average loan amount?

Let's break it down into two parts. 
* Think about the ten counties where *Wells Fargo* applications have the highest average loan amount. 
* Now, that you have the names of those counties, how many applications have been made from those counties to *Wells Fargo*.

**Information:** `county_code` in `loans` is the state and county codes concatenated together whereas `counties` have these as separate columns (as an example, 55025 is the county_code for Dane County in loans, but this will show up as `STATE=55` and `COUNTY=25` in the counties view. As such, you may find the following snippet useful when joining with `counties` 
```python
...
ON loans.county_code = counties.STATE*1000 + counties.COUNTY
...
```

Answer Q7 with a Python `dict` that looks like this:

```python
{'Sawyer': 38,
 'Door': 174,
 'Forest': 7,
 'Ozaukee': 389,
 'Bayfield': 33,
 'Waukesha': 1832,
 'Vilas': 68,
 'Dane': 729,
 'Oneida': 70,
 'Florence': 8}
```

The cell following your answer should have a plot that looks like this:

<img src="q7.png" width=500>

The bars are sorted by the average loan amount in each county (for
example, applications having the highest average loan amount are from Sawyer, Door is second most,
etc).

#### Q8: When computing a MEAN aggregate per group of loans, under what situation (when) do we require network I/O between the `partial_mean` and `mean` operations?

Write some simple `GROUP BY` queries on `loans` and call `.explain()`.
Try grouping by the `county_code`. Then try grouping by the `lei`
column.

If a network transfer (network I/O) is necessary for one query but not the other,
write a comment explaining why.  You might want to look back at how
you loaded the data to a Hive table earlier.

Write your answer in a cell like the following.

```python
#q8
# .. your ..
# .. answers ..
```

## :robot: Part 4: Machine Learning

The objective of Part 4 is to use the given loan dataset to train a
Decision Tree model that can predict outcomes of loan applications
(approved or not). Recall that a loan is approved if `action_taken` is
"Loan originated".

We call our label `approval`, indicating whether a loan
application is approved or not (`1` for approved, `0` otherwise). And
for this exercise, we will use the features `loan_amount`, `income`,
`interest_rate` in `loans` table for prediction.

First, as a preparatory step, get the features and label from the loans
table into a new dataframe `df`. Cast the `approval`, `income` and `interest_rate`
columns to `double` type and fill missing values of all features and label columns
by 0.0.

Then split `df` as follows:

```python
# deterministic split
train, test = df.randomSplit([0.8, 0.2], seed=41) 
```

Cache the `train` DataFrame.

#### Q9. How many loans are approved (`approval = 1`) in the `train` DataFrame?
Answer with a single number.

#### Q10. What is the accuracy of the random forest classifier with 10 trees on the test dataset?

You'll need to train a decision tree first.  Start with some imports:

```python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
```

Use the VectorAssembler to combine the feature columns `loan_amount`,
`income`, `interest_rate` into a single column.

Train a `RandomForestClassifier` with 10 trees (and default arguments
for other parameters) on your training data to predict `approved`
based on the features. More about `RandomForestClassifier` is [here](https://spark.apache.org/docs/latest/ml-classification-regression.html#random-forest-classifier).

Use the model to make predictions on the test data.  What is the
*accuracy* (fraction of times the model is correct)?

## :outbox_tray: Submission

The structure of the required files for your submissions are as follows:

```
project-5-<your_team_name>
|--- boss.Dockerfile
|--- worker.Dockerfile
|--- nb
     |--- p5.ipynb
```

While grading, we will copy the other files (e.g. `docker-compose.yml`) at appropriate locations.

We should be able to run the following commands on your submission to directly create the mini cluster:

```
# builds
docker build . -f p5-base.Dockerfile -t p5-base
docker build . -f notebook.Dockerfile -t p5-nb
docker build . -f namenode.Dockerfile -t p5-nn
docker build . -f datanode.Dockerfile -t p5-dn
docker build . -f boss.Dockerfile -t p5-boss
docker build . -f worker.Dockerfile -t p5-worker
# runs the cluster
docker compose up -d
```

We should then be able to open `http://localhost:5000/lab`, find your
notebook, and run it.

## :trophy: Testing

`tester.py`, `nbutils.py`, and `autograde.py` should already be on your repository. 

### Partial Test

Full test will remove your docker containers and run your notebook on a different docker cluster. If this hinders your developer experience, you can choose to check the answers only. 

The following command will keep your cluster running. It will only check the answers of your `nb/p5.ipynb` notebook.

```bash
python3 autograde.py -s
```

### Full Test

We recommend running the full test once you have completed all the answers.

```bash
python3 autograde.py
```

Note that, while grading, we will run the full test.

### Manual Grading

Q6 and Q8 will be manually graded after your submission, so the autograder will not give you any feedback on them (it always says `PASS`)!

<!-- Of course, the checker only looks at the answers, not how you got them, so there may be further deductions (especially in the case of hardcoding answers). Moreover, Q6 and Q8 will be manually graded after your submission, so the autograder will not give you any feedback on them (it always says `PASS`)! -->
