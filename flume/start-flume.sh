#!/bin/bash
# $ bin/flume-ng agent --conf conf --conf-file example.conf --name a1

FLUME_CONF_DIR=/opt/flume/conf

[[ -d "${FLUME_CONF_DIR}"  ]]  || { echo "Flume config dir not mounted in /opt/flume-config";  exit 1; }
[[ -z "${FLUME_AGENT_NAME}" ]] && { echo "Setting FLUME_AGENT_NAME by default to a1"; FLUME_AGENT_NAME=a1; }

echo "Starting flume agent : ${FLUME_AGENT_NAME}"

flume-ng agent \
  -c ${FLUME_CONF_DIR} \
  -f ${FLUME_CONF_DIR}/${FLUME_CONF_FILE}\
  -n ${FLUME_AGENT_NAME} \
  -Dorg.apache.flume.log.printconfig=true \
  -Dorg.apache.flume.log.rawdata=true 2>&1

while :
do
	echo "Press [CTRL+C] to stop.."
	sleep 1
done

echo $?
