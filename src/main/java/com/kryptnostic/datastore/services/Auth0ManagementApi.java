package com.kryptnostic.datastore.services;

import org.json.simple.JSONObject;
import retrofit.http.Body;
import retrofit.http.GET;
import retrofit.http.PATCH;
import retrofit.http.Path;

// Internal use only! Do NOT add to JDK
public interface Auth0ManagementApi {
    String BASIC_REQUEST_FIELDS = "?fields=user_id%2Cemail%2Cnickname%2Capp_metadata";

    String USERS = "/users";

    String USER_ID      = "userId";
    String USER_ID_PATH = "/{" + USER_ID + "}";

    @GET( USERS + BASIC_REQUEST_FIELDS )
    JSONObject[] getAllUsers();

    @GET( USERS + USER_ID_PATH + BASIC_REQUEST_FIELDS )
    JSONObject getUser( @Path( USER_ID ) String userId );

    @PATCH( USERS + USER_ID_PATH )
    Void resetRolesOfUser( @Path( USER_ID ) String userId, @Body JSONObject app_metadata );
}
