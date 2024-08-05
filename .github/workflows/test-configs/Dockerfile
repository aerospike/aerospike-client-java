FROM aerospike.jfrog.io/docker/aerospike/aerospike-proxy-private:1.1.0
# Default user doesn't have root permissions
USER root
RUN curl "https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64" -o /usr/bin/yq
RUN chmod u+x /usr/bin/yq
RUN yq -i e 'del(.security)' /etc/aerospike-proxy/aerospike-proxy.yml
