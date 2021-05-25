FROM ubuntu:20.04

RUN apt-get update; apt-get install -y openjdk-11-jre-headless; apt-get clean
ADD rdf-delta-${version}.tar /opt
ENV DELTA_HOME=/opt/rdf-delta-${version}
ENTRYPOINT [ "/opt/rdf-delta-${version}/dcmd" ]
CMD [ "server", "-mem" ]
