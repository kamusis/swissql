FROM maven:3.9.6-eclipse-temurin-21 AS build

WORKDIR /workspace

COPY pom.xml ./
RUN mvn -B -q -DskipTests dependency:go-offline

COPY src ./src
RUN mvn -B -DskipTests package

RUN JAR_PATH=$(ls -1 target/*.jar | grep -v '\\.original$' | head -n 1) && \
    test -n "$JAR_PATH" && \
    cp "$JAR_PATH" /workspace/swissql-server.jar

FROM eclipse-temurin:21-jre

WORKDIR /app

COPY --from=build /workspace/swissql-server.jar /app/swissql-server.jar

EXPOSE 8080

ENV JAVA_OPTS=""
ENV SPRING_CONFIG_ADDITIONAL_LOCATION="optional:file:/config/"
ENV SPRING_CONFIG_IMPORT="optional:configtree:/run/secrets/"

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/swissql-server.jar"]
