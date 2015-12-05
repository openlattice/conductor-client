package com.kryptnostic.conductor.orchestra;

import com.kryptnostic.conductor.v1.objects.ServiceDescriptor;

import retrofit.http.Body;
import retrofit.http.GET;
import retrofit.http.POST;

public interface ConductorApi {

    final String MONITORING   = "/monitoring";

    final String REGISTRATION = "/registration";
    final String HEALTH       = "/health";

    @POST( MONITORING + REGISTRATION )
    void setRegistration( @Body ServiceDescriptor desc );

    @GET( MONITORING + HEALTH )
    void checkHealth();

}
