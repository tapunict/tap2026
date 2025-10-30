# Creating first container to compile Java code and create jar file
FROM maven:3-openjdk-11 AS build
WORKDIR /app

# Copying pom.xml file to container
COPY kafka-streams.examples/pom.xml .

# Downloading dependencies
RUN mvn dependency:go-offline

COPY kafka-streams.examples/src src

RUN mvn -f ./pom.xml package

FROM openjdk:11-slim
ENV BUILD="kafka-streams.examples-0.1-jar-with-dependencies.jar"
WORKDIR /app
COPY --from=build /app/target/${BUILD} app.jar  
CMD ["java", "-cp", "/app/app.jar", "tap.WordCount", "/tmp/kafka-streams"]
