FROM apache/spark:latest

ADD build/libs/sparkApp-1.0-SNAPSHOT.jar /opt/sparkApp-1.0-SNAPSHOT.jar


ENV SPARK_APPLICATION_JAR_LOCATION /opt/sparkApp-1.0-SNAPSHOT.jar
ENV PATH="/opt/spark/bin:${PATH}"

CMD spark-submit \
    --class org.example.SimpleApp \
    $SPARK_SUBMIT_ARGS \
    $SPARK_APPLICATION_JAR_LOCATION