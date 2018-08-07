FROM continuumio/anaconda3
MAINTAINER "Predmac Technologies"

RUN apt-get update && apt-get install -y software-properties-common
RUN add-apt-repository ppa:webupd8team/java 
RUN apt-get update && \
    echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections && \
    apt-get install -y oracle-java8-installer --allow-unauthenticated
    
COPY . /


RUN apt-get update && apt-get install -y libgtk2.0-dev && \
    apt-get update && \
    apt-get install  nano && \
    
    rm -rf /var/lib/apt/lists/* && \
    /opt/conda/bin/conda install jupyter -y && \

    /opt/conda/bin/conda install -c conda-forge py4j=0.10.7 && \
    /opt/conda/bin/conda install -c anaconda joblib && \
    pip install pyspark && \
    pip install tqdm && \
    pip install sklearn && \
    pip install pymysql && \
    pip install matplotlib && \
    pip install mysql-connector-python && \
    /opt/conda/bin/conda install -c conda-forge fbprophet 
    
# Install cron
RUN apt-get update && apt-get -y install cron

RUN apt-get update

# Add crontab file in the cron directory
ADD crontab /etc/cron.d/simple-cron

# Add shell script and grant execution rights
ADD script.sh /script.sh
RUN chmod +x /script.sh

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/simple-cron

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

# Run the command on container startup
CMD cron && tail -f /var/log/cron.log

