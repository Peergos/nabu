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

ENV IPFS_PATH=/opt/nabu/.ipfs

WORKDIR /opt/nabu
RUN mkdir -p /opt/nabu/.ipfs
COPY --from=build /opt/nabu/target /opt/nabu

ENTRYPOINT ["java", "-cp", "/opt/nabu/nabu-v0.0.1-SNAPSHOT-jar-with-dependencies.jar", "org.peergos.Server", "Addresses.API", "/ip4/0.0.0.0/tcp/5001"]

EXPOSE 4001 5001 8080 8000
