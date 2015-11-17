package com.kryptnostic.conductor.orchestra;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.conductor.orchestra.NameConstants;

public class ServiceDescriptor{
	
	private final String           serviceName;
    private final String           serviceHost;
    private final int              servicePost;
    private final String		   servicePingbackUrl;
    private final String		   serviceDeployPath;

    @JsonCreator
    public ServiceDescriptor(
            @JsonProperty( NameConstants.SERVICE_NAME_FIELD ) 			String serviceName,
            @JsonProperty( NameConstants.SERVICE_HOST_FIELD ) 			String serviceHost,
            @JsonProperty( NameConstants.SERVICE_PORT_FIELD ) 			int servicePost,
            @JsonProperty( NameConstants.SERVICE_PINGBACK_URL_FIELD )	String servicePingbackUrl,
            @JsonProperty( NameConstants.SERVICE_DEPLOY_PATH_FIELD ) 	String serviceDeployPath){
        this.serviceName = serviceName;
        this.serviceHost = serviceHost;
        this.servicePost = servicePost;
        this.servicePingbackUrl = servicePingbackUrl;
        this.serviceDeployPath  = serviceDeployPath;  
    }


    @JsonProperty( NameConstants.SERVICE_NAME_FIELD )
	public String getServiceName() {
		return serviceName;
	}


    @JsonProperty( NameConstants.SERVICE_HOST_FIELD )
	public String getServiceHost() {
		return serviceHost;
	}


    @JsonProperty( NameConstants.SERVICE_PORT_FIELD )
	public int getServicePort() {
		return servicePost;
	}


    @JsonProperty( NameConstants.SERVICE_PINGBACK_URL_FIELD )
	public String getServicePingbackUrl() {
		return servicePingbackUrl;
	}


    @JsonProperty( NameConstants.SERVICE_DEPLOY_PATH_FIELD )
	public String getServiceDeployPath() {
		return serviceDeployPath;
	}


	@Override
	public String toString() {
		return "ServiceDescriptor [serviceName=" + serviceName + ", serviceHost=" + serviceHost + ", servicePost="
				+ servicePost + ", servicePingbackUrl=" + servicePingbackUrl + ", serviceDeployPath="
				+ serviceDeployPath + "]";
	}
	
    

}
