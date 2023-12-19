gcloud commands:

>> Dataproc w/o auto
gcloud dataproc clusters create spark35-testing-w3l-e2-high --region us-central1 --master-machine-type e2-standard-16 --master-boot-disk-size 500 --num-workers 2 --worker-machine-type e2-standard-16 --worker-boot-disk-size 500 --image-version 2.2-debian12
gcloud dataproc jobs submit spark --cluster=spark35-testing-w3l-e2-high --class=com.example.SparkSapBomTemplateWithExplosion --jars=gs://spark_scala_testing/scala_app/spark-poc-12.2-SNAPSHOT.jar --region=us-central1  --properties=spark:spark.executor.memory=2g,spark:spark.driver.memory=2g -- GCS large dataprocW3large1502


>> Dataproc w/ auto
gcloud dataproc clusters create spark35-testing-w3l-e2-high-auto --autoscaling-policy CDF_AUTOSCALING_POLICY_V1 --region us-central1 --master-machine-type e2-standard-16 --master-boot-disk-size 500 --num-workers 2 --worker-machine-type e2-standard-16 --worker-boot-disk-size 500 --image-version 2.2-debian12
gcloud dataproc jobs submit spark --cluster=spark35-testing-w3l-e2-high-auto --class=com.example.SparkSapBomTemplateWithExplosion --jars=gs://spark_scala_testing/scala_app/spark-poc-12.2-SNAPSHOT.jar --region=us-central1 --properties=spark:spark.executor.memory=2g,spark:spark.driver.memory=2g  -- GCS large dataprocAutoW3large1503


>> Serverless Dataproc
gcloud dataproc batches submit spark --class=com.example.SparkSapBomTemplateWithExplosion --jars=gs://spark_scala_testing/scala_app/spark-poc-12.1-SNAPSHOT.jar --region=us-central1 --version=2.2 -- GCS small dataprocServerlessW3small1452