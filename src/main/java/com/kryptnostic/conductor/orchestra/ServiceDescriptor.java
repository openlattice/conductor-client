package com.kryptnostic.conductor.orchestra;

import com.fasterxml.jackson.annotation.JsonCreator;

public class ServiceDescriptor{
	
	private final String           name;
    private final String           host;
    private final String		   pingbackUrl;
    private final int              port;


    @JsonCreator
    public ServiceDescriptor(
            String name,
            String host,
            String pingbackUrl,
            int port) {
        this.name = name;
        this.host = host;
        this.pingbackUrl = pingbackUrl;
        this.port = port;
    }

    public String getName() {
        return name;
    }

    public String getHost() {
        return host;
    }

    public String getPingbackUrl() {
        return pingbackUrl;
    }

    public int getPort() {
        return port;
    }

	@Override
	public String toString() {
		return "ServiceDescriptor [name=" + name + ", host=" + host + ", pingbackUrl=" + pingbackUrl + ", port=" + port
				+ "]";
	}
    

}
