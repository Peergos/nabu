FROM eclipse-temurin:17-jdk as build

RUN apt-get update && apt-get install --assume-yes maven git

COPY . /opt/nabu
WORKDIR /opt/nabu

RUN mvn clean package -DskipTests

FROM eclipse-temurin:17-jre

LABEL org.opencontainers.image.title="Nabu"
LABEL org.opencontainers.image.description="Nabu is a minimal Java implementation of IPFS"
LABEL org.opencontainers.image.source="https://github.com/Peergos/nabu"
LABEL org.opencontainers.image.licenses="MIT license"

WORKDIR /opt/nabu

COPY --from=build /opt/nabu/target /opt/nabu

ENTRYPOINT ["java", "-cp", "/opt/nabu/nabu-v0.8.1-jar-with-dependencies.jar", "org.peergos.client.InteropTestClient"]

EXPOSE 4001
