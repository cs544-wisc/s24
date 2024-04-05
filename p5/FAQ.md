# FAQ (P5 Spark)

## Cannot find `setup.sh`

It will be available once you accept the classroom invitation link for P5. After you have created a team, you will get a repository. Clone the repository in your VM. `setup.sh` will be there.

## Spark master and/or worker containers start, but they die after some time

Note that `start-master.sh` initiates the master in the background. 

> If the script starts the process in the background or if the process terminates for some reason, the container will stop since Docker containers are designed to stop when their main process exits.

To keep a container running, you have to start a foreground process using CMD. The same goes for `start-worker.sh`. Go over the lectures on this.

## Spark queries are too slow

Most probably, the swap has not been increased.

Fix: Go through the README to increase the swap size. Then, start the cluster again.

## VM went unresponsive

To make the VM responsive again, reset your VM from Google Cloud Console. Resetting will reboot your VM (the external IP will be the same, and all the files will be there).

## VM went unresponsive in the middle of a spark job

It happens when the spark has occupied almost all of the VM's memory. This is expected in p5. This is the reason behind the increasing swap size.

> A swap file is a system file that creates temporary storage space on a solid-state drive or hard disk when the system runs low on memory. The file swaps a section of RAM storage from an idle program and frees up memory for other programs. ([techtarget](https://www.techtarget.com/searchwindowsserver/definition/swap-file-swap-space-or-pagefile))

Most probably, the swap has not been increased.

Fix: To make the VM responsive again, reset your VM from Google Cloud Console. Resetting will reboot your VM (the external IP will be the same, and all the files will be there). Go through the README to increase the swap size. Then, start the cluster again.

## Error while uploading CSV files to HDFS
There could be a couple of issues.

### 1. CSV files not found
Check if the files are available in `nb/data` folder within jupyterlab. Try again.

### 2. HDFS error
The HDFS cluster is probably not ready yet. Check that using 

```
!hdfs dfsadmin -fs hdfs://nn:9000/ -report
```

It should show `Live datanodes (1)` when HDFS is ready. Then try again.


## Q7 - Are we allowed to hardcode `lei`?
No.

## Q7 - The plot looks similar but not exactly the same
if the values are the same, then you are okay.

## Q9 - Count is close but not equal

This could be due to the following issues:
1. The order of the columns is not as we intended. Check if the columns are in this order, `"loan_amount", "income", "interest_rate", "approval"` before splitting the data into train-test.
2. The `approval` column is not binary, which is contrary to what we intended. Before splitting, the `approval` column must have only 0s and 1s.
3. You are working with loans **dataframe** which has different row order than `loans` table. According to README, "get the features and label from the `loans` **table** into a new dataframe `df`". The loans *table* and the loans *dataframe* are seperate entities. So, initialize `df` from `loans` table using `spark.sql`.
4. You may have accidentally created a temporary view named `loans`. This will create a new view with a different row order. This will also replace the `loans` table from table namespace. You can check by running `q4` again. You are only supposed to create views for [these](./README.md#other-views).
3. The data order somehow got changed before splitting into train and test. Hence, the train dataset is a different set of rows than we intended.
4. Some rows mistakenly got deleted while handling missing values.
5. There is an error in counting how many loans are approved.


Also, the first few rows of data should look like this before splitting. You can find this by running `df.show()`

```
+-----------+------+-------------+--------+
|loan_amount|income|interest_rate|approval|
+-----------+------+-------------+--------+
|   255000.0| 210.0|          0.0|     1.0|
|   435000.0|   0.0|        3.125|     0.0|
|   435000.0| 190.0|          0.0|     1.0|
|   165000.0|   0.0|         3.25|     0.0|
|   205000.0|   0.0|          0.0|     1.0|
|   305000.0|   0.0|          3.5|     0.0|
|   195000.0|  43.0|         2.75|     1.0|
|   185000.0|   0.0|          3.5|     0.0|
|   265000.0|  93.0|         2.75|     1.0|
|   185000.0|   0.0|         3.25|     0.0|
|   475000.0|   0.0|        1.999|     1.0|
|   235000.0|   0.0|         3.25|     0.0|
|   425000.0|   0.0|          2.0|     1.0|
|   235000.0|   0.0|         3.25|     0.0|
|   185000.0|   0.0|         2.25|     1.0|
|   385000.0|   0.0|          3.0|     0.0|
|   365000.0| 321.0|        2.625|     1.0|
|   155000.0|   0.0|          2.5|     0.0|
|   245000.0|   0.0|         3.75|     1.0|
|   315000.0|   0.0|          3.0|     0.0|
+-----------+------+-------------+--------+
only showing top 20 rows
```


## Q10 - Accuracy is too high/low
Check if Q9 is correct. If it is, check your logic of measuring accuracy. Or, use one of the evaluators of the `pyspark` library. Also, check if you have preprocessed the data correctly. 
