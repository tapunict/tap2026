FROM python:3.10-bullseye

# Install Java 11 
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# See https://sparknlp.org/docs/en/install#python
# Install Spark NLP and PySpark with dependencies
RUN pip install numpy spark-nlp==6.2.0 pyspark==3.3.1

# Copy application source code
# COPY src /usr/src/app

# Set working directory
WORKDIR /usr/src/app

# Set entrypoint to allow passing script name dynamically
# ENTRYPOINT ["python3"]
CMD ["python example.py"]

