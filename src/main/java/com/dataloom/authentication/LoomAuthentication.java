package com.dataloom.authentication;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;

import com.auth0.spring.security.api.Auth0JWTToken;
import com.auth0.spring.security.api.Auth0UserDetails;
import com.dataloom.authorization.ForbiddenException;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;

public class LoomAuthentication implements Authentication {
    private static final Logger  logger           = LoggerFactory.getLogger( LoomAuthentication.class );
    private static final long    serialVersionUID = -6853527586490225640L;
    private final Principal      principal;
    private final Set<Principal> principals;
    private final Auth0JWTToken  jwtToken;

    public LoomAuthentication( Authentication authentication ) {
        if ( authentication != null
                && Auth0JWTToken.class.isAssignableFrom( authentication.getClass() )
                && authentication.isAuthenticated() ) {
            jwtToken = (Auth0JWTToken) authentication;
            Auth0UserDetails details = (Auth0UserDetails) jwtToken.getPrincipal();
            final Collection<? extends GrantedAuthority> authorities = details.getAuthorities();
            Object principalId = details.getAuth0Attribute( LoomAuth0AuthenticationProvider.SUBJECT_ATTRIBUTE );

            if ( principalId == null ) {
                principalId = details.getAuth0Attribute( LoomAuth0AuthenticationProvider.USER_ID_ATTRIBUTE );
            }

            principal = new Principal( PrincipalType.USER, principalId.toString() );
            principals = new HashSet<>( authorities.size() + 1 );
            principals.add( principal );

            authorities
                    .stream()
                    .map( authority -> new Principal( PrincipalType.ROLE, authority.getAuthority() ) )
                    .collect( Collectors.toCollection( () -> principals ) );
        } else {
            logger.debug( "Authentication failed for authentication: {}", authentication );
            throw new ForbiddenException( "Unable to authorize access to requested resource." );
        }
    }

    public Principal getLoomPrincipal() {
        return principal;
    }

    public Set<Principal> getLoomPrincipals() {
        return principals;
    }

    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
        return jwtToken.getAuthorities();
    }

    @Override
    public Object getPrincipal() {
        return jwtToken.getPrincipal();
    }

    public String getJwt() {
        return jwtToken.getJwt();
    }

    public Object getCredentials() {
        return jwtToken.getCredentials();
    }

    public String getName() {
        return jwtToken.getName();
    }

    public boolean isAuthenticated() {
        return jwtToken.isAuthenticated();
    }

    public void setAuthenticated( boolean authenticated ) {
        jwtToken.setAuthenticated( authenticated );
    }

    public Object getDetails() {
        return jwtToken.getDetails();
    }

    public void setDetails( Object details ) {
        jwtToken.setDetails( details );
    }

    public void eraseCredentials() {
        jwtToken.eraseCredentials();
    }

    public boolean equals( Object obj ) {
        return jwtToken.equals( obj );
    }

    public int hashCode() {
        return jwtToken.hashCode();
    }

    public String toString() {
        return jwtToken.toString();
    }
}
