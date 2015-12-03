package com.kryptnostic.conductor.orchestra;

import com.kryptnostic.conductor.v1.objects.ServiceDescriptor;

import retrofit.http.Body;
import retrofit.http.GET;
import retrofit.http.POST;

public interface ConductorApi {

    final String REGISTRATION = "/registration";
    final String HEALTH       = "/health";

    @POST( REGISTRATION )
    void setRegistration( @Body ServiceDescriptor desc );

    @GET( HEALTH )
    void checkHealth();

}
