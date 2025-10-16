FROM docker.elastic.co/logstash/logstash:9.1.5

# Add your logstash plugins setup here
RUN logstash-plugin install logstash-filter-sentimentalizer 

# to build with
# docker build . --tag tap:logstash
# and run with
# docker run --rm -it -v $PWD/pipeline/:/usr/share/logstash/pipeline/ tap:logstash
# make sure that pipeline dir with proper conf is there%
