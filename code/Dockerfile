# Usamos la imagen jupyter/scipy-notebook como base
FROM jupyter/scipy-notebook:latest

USER root

# Actualizamos e instalamos dependencias
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y vim openjdk-11-jdk && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

# Agregamos extras de Jupyter (incluyendo Spark)
RUN conda install --quiet --yes \
    'pyspark' \
    'cassandra-driver' && \
    conda clean --all -f -y

RUN pip install --no-cache-dir kafka-python

# Instalar paquetes de Python adicionales si es necesario
# RUN pip install --no-cache-dir mypackage1 mypackage2 ...

# Crear un directorio de trabajo y cambiar el propietario a jovyan (usuario predeterminado de jupyter)
RUN mkdir /home/jovyan/work && chown jovyan:users /home/jovyan/work
USER jovyan

# Establecer el directorio de trabajo
WORKDIR /home/jovyan/work

# Copiamos nuestros notebooks y otros archivos de código al contenedor
COPY code/* /home/jovyan/work/