package com.kryptnostic.conductor.orchestra;


import com.kryptnostic.conductor.v1.objects.ServiceDescriptor;

import retrofit.http.Body;
import retrofit.http.GET;
import retrofit.http.POST;

public interface ConductorApi {
	
	final String CONTROLLER 		= "/conductor";
	final String REGISTRATION 		= "/registration";
	final String HEALTH				= "/health";
	
	
	@POST( CONTROLLER + REGISTRATION)
	void setRegistration(@Body ServiceDescriptor desc);

	@GET( CONTROLLER + HEALTH)
	void checkHealth();
}
