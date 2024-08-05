FROM aerospike.jfrog.io/docker/aerospike/aerospike-proxy-private:1.1.0
# Default user "aerospike" doesn't have root permissions
USER root
RUN wget https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 -O /usr/bin/yq
RUN chmod +x /usr/bin/yq
RUN yq -i e 'del(.security)' /etc/aerospike-proxy/aerospike-proxy.yml
RUN yq -i e '.aerospike.seeds[0].native-server = .aerospike.seeds[0].localhost' /etc/aerospike-proxy/aerospike-proxy.yml
RUN yq -i e 'del(.aerospike.seeds[0].localhost)' /etc/aerospike-proxy/aerospike-proxy.yml
