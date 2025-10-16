FROM ubuntu
ARG HADOOP_VERSION=3.4.2
ARG USER=hdfs
ARG GROUP=hadoop
ARG HADOOP_DOWNLOAD_URL="https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz"

RUN apt -qq update && \
	apt -qq install -y \
		openjdk-8-jre-headless \
		libsnappy1v5 \
		libssl-dev \
		libzstd1 \
		xmlstarlet \
		curl \
		jq \
        net-tools \
	&& rm -rf /var/lib/apt/lists/*

RUN groupadd ${GROUP}  \
&& useradd --home-dir /opt/hadoop --gid ${GROUP} --no-create-home --shell /bin/bash ${USER} 


# ADD ${HADOOP_DOWNLOAD_URL} /opt
RUN cd /opt \
    && curl -o ./hadoop-${HADOOP_VERSION}.tar.gz ${HADOOP_DOWNLOAD_URL} \
    && tar xzf ./hadoop-${HADOOP_VERSION}.tar.gz \
    && rm ./hadoop-${HADOOP_VERSION}.tar.gz \
	&& ln -s ./hadoop-${HADOOP_VERSION} ./hadoop \
	&& mkdir -p -m 755 /var/log/hadoop /dfs\
	&& chown ${USER}:${GROUP} /var/log/hadoop /dfs 

USER ${USER}

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre/
ENV HADOOP_HOME=/opt/hadoop
ENV HOME=/opt/hadoop
ENV HADOOP_LOG_DIR=/var/log/hadoop
ENV PATH=$HADOOP_HOME/bin:$PATH

COPY conf/core-site.xml  conf/hdfs-site.xml ${HADOOP_HOME}/etc/hadoop/
CMD hadoop