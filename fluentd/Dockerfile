FROM fluentd

USER root
RUN apt-get update && apt-get install -y ruby-dev

RUN fluent-gem install fluent-plugin-kafka