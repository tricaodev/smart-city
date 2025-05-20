# Smart City
* Ingest data from IOT devices to AWS
* System Architecture
![system_architecture](https://github.com/user-attachments/assets/deb389e6-7f80-46e3-96e9-6c05e8ef9649)

# How to run app
## 1. Build data pipeline system
* ```docker-compose up -d```
## 2. Generate data to kafka topics
* ```python jobs/main.py```
## 3. Create S3 Bucket in AWS
## 4. Create IAM user with ACCESS_KEY and SECRET_KEY to connect spark with S3 bucket
## 5. Run spark job
* ```docker exec -it spark-master bash```
* ```spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk:1.12.783 jobs/smart_city.py```
## 6. Create database in Glue to connect with data of S3 bucket and view data by Aws Athena
## 7. Connect data with Redshift cluster
