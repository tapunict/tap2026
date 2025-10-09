FROM ubuntu
RUN apt-get update && apt-get -y install fortunes fortune-mod
WORKDIR /usr/share/games/fortunes/
ADD sicilian .
RUN strfile -c % sicilian 
ENTRYPOINT ["/usr/games/fortune"]
CMD ["sicilian"]
