Task:
In PySpark develop a film recommendation system (show top 5 film recommendations for a specific customer) based on data in a CSV file.

Description:
The program uses the alternating least squares (ALS) algorithm.

Environment:
- Hortonworks Sandbox HDP 2.6.5 on VirtualBox
- Spark 2.3.0
- PySpark

Step-by-step instructions:
1. Copy the necessary files from Windows to Sandbox:
pscp -P 2222 C:\Users\mlysikov\Downloads\repo\projects\recommendation-using-spark\recommendation.py C:\Users\mlysikov\Downloads\repo\projects\recommendation-using-spark\films.csv maria_dev@127.0.0.1:/home/maria_dev/

2. Create additional directories in HDFS:
hadoop fs -mkdir input

3. Copy the CSV file to HDFS:
hadoop fs -copyFromLocal /home/maria_dev/films.csv /user/maria_dev/input/

4. Install numpy:
yum install numpy

5. Run the PySpark script:
spark-submit reconciliation.py 1

What could be improved:
General:
In my case, on my test data, the algorithm does not show very good results. What can we do:
1. Train/test with different hyperparameters.
2. ALS - is a black box algorithm for us. In some cases, it is more reliable to write your own implementation of the film similarity algorithm which can produce much better results.
3. Generate a larger dataset or get real data. Maybe after that, we might get better results.