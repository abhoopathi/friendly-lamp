FROM continuumio/anaconda3
MAINTAINER "Extreme Networks"

RUN apt-get update && apt-get install -y software-properties-common
RUN add-apt-repository ppa:webupd8team/java 
RUN apt-get update && \
    echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections && \
    apt-get install -y oracle-java8-installer --allow-unauthenticated
    
COPY . /

#RUN /opt/conda/bin/conda install jupyter -y && \

RUN apt-get update && apt-get install -y libgtk2.0-dev && \
    apt-get update && \
    apt-get install  nano && \
    
    rm -rf /var/lib/apt/lists/* && \

    /opt/conda/bin/conda install -c conda-forge py4j=0.10.7 && \
    /opt/conda/bin/conda install -c anaconda joblib && \
    pip install pyspark && \
    pip install tqdm && \
    pip install sklearn && \
    pip install pymysql && \
    pip install schedule && \
    pip install matplotlib && \
    pip install mysql-connector-python && \
    /opt/conda/bin/conda install -c conda-forge fbprophet 

RUN chmod +x /script.sh   
CMD ["/script.sh"]
