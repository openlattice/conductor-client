package com.kryptnostic.conductor.orchestra;



public class ServiceStatus {

	private final String           name;
    private final String           host;
    private final int              port;
    private final String 		   path;
    private final boolean    	   connectable;
    
	public ServiceStatus(String name, String host, int port, String path, boolean connectable) {
		super();
		this.name = name;
		this.host = host;
		this.port = port;
		this.connectable = connectable;
		this.path = path;
	}

	public String getName() {
		return name;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public boolean isConnectable() {
		return connectable;
	}

	public String getPath() {
		return path;
	}

	@Override
	public String toString() {
		return "ServiceStatus [name=" + name + ", host=" + host + ", port=" + port + ", path=" + path + ", connectable="
				+ connectable + "]";
	}
    
}
