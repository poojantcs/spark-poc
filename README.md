gcloud commands:

>> Dataproc w/o auto
gcloud dataproc clusters create spark332-testing-e2-small --region us-central1 --master-machine-type e2-standard-4 --master-boot-disk-size 500 --num-workers 2 --worker-machine-type e2-standard-4 --worker-boot-disk-size 500 --image-version 2.1-debian11
gcloud dataproc jobs submit spark --cluster=spark332-testing-e2-small --class=com.example.SparkSapBomTemplateW4 --jars=gs://spark_scala_testing/scala_app/spark-poc-0.0.2-DATAPROC.jar --region=us-central1  --properties="spark.executor.memory"="2g","spark.driver.memory"="2g","spark.executor.cores"="1","spark.driver.cores"="1" -- GCS large dataprocW4l040124


>> Dataproc w/ auto
gcloud dataproc clusters create spark332-testing-e2-large-auto-new --autoscaling-policy CDF_AUTOSCALING_POLICY_V1 --region us-central1 --master-machine-type e2-standard-16 --master-boot-disk-size 500 --num-workers 2 --worker-machine-type e2-standard-16 --worker-boot-disk-size 500 --image-version 2.1-debian11
gcloud dataproc jobs submit spark --cluster=spark332-testing-e2-large-auto-new --class=com.example.SparkSapBomTemplateW4 --jars=gs://spark_scala_testing/scala_app/spark-poc-0.0.2-DATAPROC.jar --region=us-central1 --properties="spark.executor.memory"="12g","spark.driver.memory"="12g","spark.executor.cores"="4","spark.driver.cores"="4"  -- GCS large dataprocAutoW4l060124

--properties="spark.submit.deployMode"="cluster","spark.dynamicAllocation.enabled"="true","spark.shuffle.service.enabled"="true","spark.executor.memory"="15g","spark.driver.memory"="16g","spark.executor.cores"="5"


>> Serverless Dataproc
gcloud dataproc batches submit spark --class=com.example.SparkSapBomTemplateW4 --jars=gs://spark_scala_testing/scala_app/spark-poc-0.0.2-SERVERLESS-DATAPROC.jar --region=us-central1 --version=2.2 --properties=spark.reducer.fetchMigratedShuffle.enabled=true,spark.dataproc.scaling.version=2 -- GCS large dataprocServerlessW4l040124292929
