#create new container from the specified base image
#install java
FROM eclipse-temurin:23-jdk AS builder

ARG APP_DIR=/app

#set the working directory, like "cd XX" into the directory
#any directive after this will be performed inside this directory
WORKDIR ${APP_DIR}

#add these files and directories into compile_dir
COPY ./mvnw .
COPY .mvn .mvn

COPY pom.xml .
COPY src src

#build the JAR file
#if successful, the JAR is built in ./target/{filename}-0.0.1-SNAPSHOT.jar
RUN ./mvnw package -Dmaven.test.skip=true

#set environment variable
ENV SERVER_PORT=4000

#tell docker that the application is listening on ${SERVER_PORT}
EXPOSE ${SERVER_PORT}

# second stage
FROM eclipse-temurin:23-jdk

ARG APP_DIR=/app

WORKDIR ${APP_DIR}

#copy from first container
#COPY --from=builder /compiledir/target/vttp5b-ssf-day18l-0.0.1-SNAPSHOT.jar vttp5b-ssf-day18l.jar
COPY --from=builder /app/target/movies-0.0.1-SNAPSHOT.jar app.jar

ENV SERVER_PORT=4000

EXPOSE ${SERVER_PORT}

#command to execute when the container starts
#ENTRYPOINT SERVER_PORT=${PORT} java -jar target/day12-0.0.1-SNAPSHOT.jar 
ENTRYPOINT java -jar app.jar