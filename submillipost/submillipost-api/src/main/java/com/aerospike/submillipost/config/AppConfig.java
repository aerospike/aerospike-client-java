package com.aerospike.submillipost.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "submillipost")
public class AppConfig {

    private int feedTtlDays;
    private int notificationTtlDays;
    private final Aerospike aerospike = new Aerospike();

    public int getFeedTtlDays() { return feedTtlDays; }
    public void setFeedTtlDays(int feedTtlDays) { this.feedTtlDays = feedTtlDays; }

    public int getNotificationTtlDays() { return notificationTtlDays; }
    public void setNotificationTtlDays(int notificationTtlDays) { this.notificationTtlDays = notificationTtlDays; }

    public Aerospike getAerospike() { return aerospike; }

    public static class Aerospike {
        private String host;
        private int port;
        private String namespace;
        /** When true, client uses alternate service/peers info commands (some cloud / multi-homed setups). */
        private boolean useServicesAlternate;

        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }

        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }

        public String getNamespace() { return namespace; }
        public void setNamespace(String namespace) { this.namespace = namespace; }

        public boolean isUseServicesAlternate() { return useServicesAlternate; }
        public void setUseServicesAlternate(boolean useServicesAlternate) {
            this.useServicesAlternate = useServicesAlternate;
        }
    }
}
