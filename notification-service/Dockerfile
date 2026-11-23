# Runs the jar that `./mvnw package` built. Build from this folder: docker build -t orderflow/notification-service .
FROM eclipse-temurin:25-jre-alpine
RUN addgroup -S app && adduser -S app -G app
USER app
WORKDIR /app
COPY target/notification-service-*-SNAPSHOT.jar app.jar
EXPOSE 8084
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
