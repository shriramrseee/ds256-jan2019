FROM ubuntu
ENV container docker

RUN apt-get update && apt-get upgrade -y

RUN apt-get -y install python-pip
RUN apt-get -y install wget
RUN apt-get -y install unzip
RUN apt-get -y install default-jre
RUN apt-get -y install default-jdk

RUN pip install futures
RUN pip install gremlinpython

RUN wget http://mirrors.estointernet.in/apache/tinkerpop/3.4.0/apache-tinkerpop-gremlin-server-3.4.0-bin.zip

RUN unzip apache-tinkerpop-gremlin-server-3.4.0-bin.zip
