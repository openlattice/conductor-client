package com.kryptnostic.datastore;

public class Constants {

    private Constants () {}
    
    //User in this role can create new roles in backend, and modify their rights. Should sync with Auth0
    public static final String ROLE_ADMIN = "Admin";
}
