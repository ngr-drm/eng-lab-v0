FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /app
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN chmod +x mvnw && ./mvnw dependency:go-offline -q
COPY src ./src
RUN ./mvnw package -DskipTests -q

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar
EXPOSE 8080
ENV JAVA_OPTS="-XX:+UseSerialGC \
               -Xms64m \
               -Xmx80m \
               -XX:MaxMetaspaceSize=80m \
               -XX:ReservedCodeCacheSize=24m \
               -XX:+TieredCompilation \
               -XX:TieredStopAtLevel=1 \
               -XX:+ExitOnOutOfMemoryError \
               -Xss256k \
               -Djava.security.egd=file:/dev/./urandom"
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]