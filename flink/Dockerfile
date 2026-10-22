# Flink image with the Tidemark jobs, the Postgres JDBC driver in lib/
# (parent classloader; see docs/schema-evolution.md) and the S3 plugin enabled.
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY schemas ./schemas
COPY flink/pom.xml ./flink/pom.xml
RUN cd flink && mvn -q -B dependency:go-offline -DskipTests || true
COPY flink/src ./flink/src
RUN cd flink && mvn -q -B package -DskipTests -Dtidemark.build.dir=/src/out \
 && mvn -q -B dependency:copy -Dartifact=org.postgresql:postgresql:42.7.7 -DoutputDirectory=/src/out/lib

FROM flink:1.20.3-scala_2.12-java17
RUN mkdir -p /opt/flink/plugins/s3-fs-presto && cp /opt/flink/opt/flink-s3-fs-presto-1.20.3.jar /opt/flink/plugins/s3-fs-presto/
COPY --from=build /src/out/lib/postgresql-42.7.7.jar /opt/flink/lib/
COPY --from=build /src/out/tidemark-flink.jar /opt/flink/usrlib/tidemark-flink.jar
