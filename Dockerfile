FROM openjdk:17-jdk-slim-buster
WORKDIR /app
COPY /build/libs/mqtt-broker-api-*.jar ./app.jar
ENTRYPOINT java -jar app.jar
