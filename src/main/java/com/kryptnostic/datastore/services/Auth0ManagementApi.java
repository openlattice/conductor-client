package com.kryptnostic.datastore.services;

import java.util.Map;
import java.util.Set;

import com.dataloom.directory.pojo.Auth0UserBasic;

import retrofit2.http.*;

// Internal use only! Do NOT add to JDK
public interface Auth0ManagementApi {
    String BASIC_REQUEST_FIELDS = "?fields=user_id%2Cemail%2Cnickname%2Capp_metadata";

    String USERS = "users";
    String PAGE = "page";
    String PER_PAGE = "per_page";

    String USER_ID      = "userId";
    String USER_ID_PATH = "{" + USER_ID + "}";

    @GET( USERS + BASIC_REQUEST_FIELDS )
    Set<Auth0UserBasic> getAllUsers( @Query( PAGE ) int page , @Query( PER_PAGE ) int perPage ) ;

    @GET( USERS + "/" + USER_ID_PATH + BASIC_REQUEST_FIELDS )
    Auth0UserBasic getUser( @Path( USER_ID ) String userId );

    @PATCH( USERS + "/" + USER_ID_PATH )
    Void resetRolesOfUser( @Path( USER_ID ) String userId, @Body Map<String,Object> app_metadata );
}
