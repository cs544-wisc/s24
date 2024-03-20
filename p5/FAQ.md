# FAQ (P5 Spark)

## Cannot find `setup.sh`

It will be available once you accept the classroom invitation link for P5. After you have created a team, you will get a respository. Clone the repository in your VM. `setup.sh` will be there.

## Spark queries are too slow

Most probably, the swap has not been increased.

Fix: Go through the README to increase the swap size. Then start the cluster again.

## VM went unresponsive

To make the VM responsive again, reset your VM from Google Cloud Console. Resetting will reboot your VM (external IP will be the same, all the files will be there).

## VM went unresponsive in the middle of a spark job

It happens when spark have occupied almost all of VM's memory. This is expected in p5. This is the reason behind increasing swap size.

> A swap file is a system file that creates temporary storage space on a solid-state drive or hard disk when the system runs low on memory. The file swaps a section of RAM storage from an idle program and frees up memory for other programs. ([techtarget](https://www.techtarget.com/searchwindowsserver/definition/swap-file-swap-space-or-pagefile))

Most probably, the swap has not been increased.

Fix: To make the VM responsive again, reset your VM from Google Cloud Console. Resetting will reboot your VM (external IP will be the same, all the files will be there). Go through the README to increase the swap size. Then start the cluster again.

## Error while uploading CSV files to HDFS

Could be a couple of issues.

### 1. CSV files not found

Check if the files are available in `nb/data` folder within jupyterlab. Try again.

### 2. HDFS Error

Probably the HDFS cluster is not ready yet. Check that using 

```
!hdfs dfsadmin -fs hdfs://nn:9000/ -report
```

It should show `Live datanodes (1)` when HDFS is ready. Then try again.

## Q9 - Count is close but not equal

This could be due to the following issues:

1. The data order somehow got changed before splitting into train and test. Hence train got a different set of rows than we intended.
2. Some rows mistakenly got deleted while handling missing values.
3. There is an error in counting how many loans are approved.

## Q10 - Accuracy is too high

Check if Q9 is correct. If it is, check your logic of measuring accuracy. Or, use one of the evaluators of the `pyspark` library.
