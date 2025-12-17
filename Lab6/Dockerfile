FROM ubuntu:22.04 AS builder

RUN apt-get update && apt-get install -y openjdk-17-jdk wget
RUN wget https://jdbc.postgresql.org/download/postgresql-42.7.7.jar -O /postgresql.jar
RUN wget https://repo1.maven.org/maven2/com/hazelcast/hazelcast/5.6.0/hazelcast-5.6.0.jar -O /hazelcast.jar

COPY CounterMapStore.java /
RUN javac -encoding UTF-8 -cp /hazelcast.jar CounterMapStore.java


FROM hazelcast/hazelcast:5.6

COPY --from=builder /postgresql.jar /opt/hazelcast/lib/
COPY --from=builder /CounterMapStore.class /opt/hazelcast/lib/
COPY hazelcast.yaml /opt/hazelcast/config/
