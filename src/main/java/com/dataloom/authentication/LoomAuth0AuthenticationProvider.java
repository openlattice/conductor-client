package com.dataloom.authentication;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;

import com.auth0.authentication.AuthenticationAPIClient;

import digital.loom.rhizome.authentication.ConfigurableAuth0AuthenticationProvider;

public class LoomAuth0AuthenticationProvider extends ConfigurableAuth0AuthenticationProvider {
    public static final String USER_ID_ATTRIBUTE = "user_id";
    public static final String SUBJECT_ATTRIBUTE = "sub";

    public LoomAuth0AuthenticationProvider( AuthenticationAPIClient auth0Client ) {
        super( auth0Client );
    }
    
    @Override
    public Authentication authenticate( Authentication authentication ) throws AuthenticationException {
        return new LoomAuthentication( super.authenticate( authentication ) );
    }
}
