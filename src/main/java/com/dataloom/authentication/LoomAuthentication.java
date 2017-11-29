/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.dataloom.authentication;

import com.auth0.spring.security.api.Auth0JWTToken;
import com.auth0.spring.security.api.Auth0UserDetails;
import com.dataloom.authorization.ForbiddenException;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.organizations.roles.SecurePrincipalsManager;
import com.openlattice.authorization.SecurablePrincipal;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collection;
import java.util.NavigableSet;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;

public class LoomAuthentication implements Authentication {

    private static final Logger logger           = LoggerFactory.getLogger( LoomAuthentication.class );
    private static final long   serialVersionUID = -6853527586490225640L;

    private final Principal               principal;
    private final NavigableSet<Principal> principals;
    private final Auth0JWTToken           jwtToken;

    public LoomAuthentication( Authentication authentication, SecurePrincipalsManager spm ) {
        if ( authentication != null
                && Auth0JWTToken.class.isAssignableFrom( authentication.getClass() )
                && authentication.isAuthenticated() ) {

            jwtToken = (Auth0JWTToken) authentication;
            Auth0UserDetails details = (Auth0UserDetails) jwtToken.getPrincipal();

            Object principalId = details.getAuth0Attribute( LoomAuth0AuthenticationProvider.SUBJECT_ATTRIBUTE );

            if ( principalId == null ) {
                principalId = details.getAuth0Attribute( LoomAuth0AuthenticationProvider.USER_ID_ATTRIBUTE );
            }

            principal = new Principal( PrincipalType.USER, principalId.toString() );

            SecurablePrincipal sp = spm.getPrincipal( principal.getId() );
            Collection<SecurablePrincipal> securablePrincipals = spm.getAllPrincipals( sp );

            principals = new TreeSet<>();
            principals.add( principal );

            securablePrincipals
                    .stream()
                    .map( SecurablePrincipal::getPrincipal )
                    .forEach( principals::add );
        } else {
            logger.debug( "Authentication failed for authentication: {}", authentication );
            throw new ForbiddenException( "Unable to authorize access to requested resource." );
        }
    }

    public Principal getLoomPrincipal() {
        return principal;
    }

    public NavigableSet<Principal> getLoomPrincipals() {
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

    @SuppressFBWarnings
    public boolean equals( Object obj ) {

        if ( obj == null ) {
            return false;
        }

        if ( !( obj instanceof LoomAuthentication ) ) {
            return false;
        }

        LoomAuthentication test = (LoomAuthentication) obj;

        if ( this.principal == null && test.principal != null ) {
            return false;
        }

        if ( this.principal != null && test.principal == null ) {
            return false;
        }

        return this.principal != null && this.principal.equals( test.principal );
    }

    public int hashCode() {
        return jwtToken.hashCode();
    }

    public String toString() {
        return jwtToken.toString();
    }
}
