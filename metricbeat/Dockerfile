FROM  docker.elastic.co/beats/metricbeat:9.2.2
COPY modules/* /usr/share/metricbeat/modules.d/
USER root
RUN chmod go-w /usr/share/metricbeat/modules.d/metricbeat.docker.yml
RUN chown root:metricbeat /usr/share/metricbeat/modules.d/metricbeat.docker.yml
USER metricbeat
