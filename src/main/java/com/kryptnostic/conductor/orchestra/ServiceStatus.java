package com.kryptnostic.conductor.orchestra;

import com.fasterxml.jackson.annotation.JsonCreator;

public class ServiceStatus {

	private final ServiceDescriptor 	service;
    private final boolean    	        connectable;
    
    @JsonCreator
	public ServiceStatus(ServiceDescriptor service, boolean connectable) {
		this.service = service;
		this.connectable = connectable;
	}
	

	public ServiceDescriptor getService() {
		return service;
	}


	public boolean isConnectable() {
		return connectable;
	}


	@Override
	public String toString() {
		return "ServiceStatus [service=" + service.toString() + ", connectable=" + connectable + "]";
	}

	
}
