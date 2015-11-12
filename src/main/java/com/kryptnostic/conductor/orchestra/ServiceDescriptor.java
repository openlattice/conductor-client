package com.kryptnostic.conductor.orchestra;

import java.net.InetAddress;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Optional;

public class ServiceDescriptor {
    private final String           name;
    private final String           host;
    private final Optional<String> healthTopic;
    private final Optional<String> pingbackUrl;
    private final int              port;
    private final InetAddress      address;

    @JsonCreator
    public ServiceDescriptor(
            String name,
            String host,
            Optional<String> healthTopic,
            Optional<String> pingbackUrl,
            int port,
            InetAddress address ) {
        this.name = name;
        this.host = host;
        this.healthTopic = healthTopic;
        this.pingbackUrl = pingbackUrl;
        this.port = port;
        this.address = address;
    }

    public String getName() {
        return name;
    }

    public String getHost() {
        return host;
    }

    public Optional<String> getHealthTopic() {
        return healthTopic;
    }

    public Optional<String> getPingbackUrl() {
        return pingbackUrl;
    }

    public int getPort() {
        return port;
    }

    public InetAddress getAddress() {
        return address;
    }

}
