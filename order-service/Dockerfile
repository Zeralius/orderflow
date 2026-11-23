# Runs the jar that `./mvnw package` built. Build from this folder: docker build -t orderflow/order-service .
FROM eclipse-temurin:25-jre-alpine
# Fixed numeric ids, so Kubernetes can check "runAsNonRoot" without trusting a user name
RUN addgroup -S -g 10001 app && adduser -S -u 10001 -G app app
USER 10001
WORKDIR /app
COPY target/order-service-*-SNAPSHOT.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
