FROM continuumio/anaconda3
MAINTAINER "predmac"

RUN apt-get update && apt-get install -y software-properties-common
RUN add-apt-repository ppa:webupd8team/java 
RUN apt-get update && \
    echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections && \
    apt-get install -y oracle-java8-installer --allow-unauthenticated
    
RUN apt-get update && apt-get install -y libgtk2.0-dev && \
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
    /opt/conda/bin/conda install -c conda-forge fbprophet && \
    pip install numpy pandas sklearn matplotlib seaborn jupyter pyyaml h5py && \
    /opt/conda/bin/conda upgrade dask 



RUN apt-get update 

RUN pip install --upgrade pip

RUN pip install numpy pandas sklearn matplotlib seaborn jupyter pyyaml h5py


RUN ["mkdir", "notebooks"]
COPY jupyter_notebook_config.py /root/.jupyter/
COPY run_jupyter.sh /
COPY mycert.pem /
COPY mykey.key /


RUN ["chmod", "+x", "/run_jupyter.sh"]
# Jupyter  ports
EXPOSE 8888 

# Store notebooks in this mounted directory
VOLUME /notebooks

CMD ["/run_jupyter.sh"]