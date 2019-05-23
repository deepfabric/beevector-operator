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

# # the general form of variable PEER_SERVICE_NAME is: "<clusterName>-hyena"
# cluster_name=`echo ${PEER_SERVICE_NAME} | sed 's/-hyena//'`
# domain="${HOSTNAME}.${PEER_SERVICE_NAME}.${NAMESPACE}.svc"

# discovery_url="${cluster_name}-discovery.${NAMESPACE}.svc:10261"
# encoded_domain_url=`echo ${domain}  | tr "\n" " " | sed "s/ //g" | base64`

# elapseTime=0
# period=1
# threshold=30
# while true; do
#     sleep ${period}
#     elapseTime=$(( elapseTime+period ))

#     if [[ ${elapseTime} -ge ${threshold} ]]
#     then
#         echo "waiting for hyena cluster ready timeout" >&2
#         exit 1
#     fi

#     if nslookup ${domain} 2>/dev/null
#     then
#         echo "nslookup domain ${domain} success"
#         break
#     else
#         echo "nslookup domain ${domain} failed" >&2
#     fi

# done

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
--prophet-urls-client=http://0.0.0.0:2371 \
--prophet-urls-advertise-client=http://${HOSTNAME}.prophet:2371 \
--prophet-urls-peer=http://0.0.0.0:2381 \
--prophet-urls-advertise-peer=http://${HOSTNAME}.prophet:2381 \
--prophet-initial-cluster=prophet-0=http://prophet-0.prophet:2381,prophet-1=http://prophet-1.prophet:2381,prophet-2=http://prophet-2.prophet:2381 \
--prophet-storage=true \
--rack=${HOSTNAME} \
--mq-addr=${MQADDR} \
--mq-topic=${MQTOPIC} \
--mq-group=${MQGROUP}\" \
"

# until result=$(wget -qO- -T 3 http://${discovery_url}/new/${encoded_domain_url} 2>/dev/null); do
#     echo "waiting for discovery service returns start args ..."
#     sleep $((RANDOM % 5))
# done
# ARGS="${ARGS}${result}"

echo "starting hyena prophet..."
sleep $((RANDOM % 10))
echo "/bin/sh ${ARGS}"
exec /bin/sh ${ARGS}
