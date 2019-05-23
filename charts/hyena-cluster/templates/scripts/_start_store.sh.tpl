#!/bin/sh

# This script is used to start hyena containers in kubernetes cluster

set -uo pipefail
ANNOTATIONS="/etc/podinfo/annotations"

if [[ ! -f "${ANNOTATIONS}" ]]
then
    echo "${ANNOTATIONS} does't exist, exiting."
    exit 1
fi
source ${ANNOTATIONS} 2>/dev/null

runmode=${runmode:-normal}
if [[ X${runmode} == Xdebug ]]
then
    echo "entering debug mode."
    tail -f /dev/null
fi


MQADDR="172.19.0.18:9092"
MQTOPIC="hyena"
MQGROUP="hyena"

ARGS="-c \
\"/usr/local/bin/hyena \
--data=/data \
--addr=0.0.0.0:9527 \
--addr-raft=0.0.0.0:9528 \
--prophet-name=${HOSTNAME} \
--prophet-addr=0.0.0.0:9529 \
--prophet-addr-client=http://prophet-0.prophet:2371,http://prophet-1.prophet:2371,http://prophet-2.prophet:2371
--prophet-storage=false \
--rack=${HOSTNAME} \
--mq-addr=${MQADDR} \
--mq-topic=${MQTOPIC} \
--mq-group=${MQGROUP}\" \
"

echo "starting hyena store..."
sleep $((RANDOM % 10))
echo "/bin/sh ${ARGS}"
exec /bin/sh ${ARGS}
