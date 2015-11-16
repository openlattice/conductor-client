package com.kryptnostic.conductor.orchestra;

import retrofit.http.PUT;

import retrofit.http.Body;

public interface ConductorApi {
	
	final String CONTROLLER 		= "/conductor";
	final String REGISTRATION 		= "/registration";
	
	
	@PUT( CONTROLLER + REGISTRATION)
	void setRegistration(@Body ServiceDescriptor desc);
	
}
