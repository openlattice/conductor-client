package com.dataloom.authorization;

import static com.google.common.base.Preconditions.checkState;

import java.util.Set;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import com.dataloom.authentication.LoomAuthentication;

public final class Principals {
    private static final Logger logger            = LoggerFactory.getLogger( Principals.class );
    public static enum Role {
        ADMIN( "admin" ),
        USER( "user" ),
        AUTHENTICATED_USER( "AuthenticatedUser" );
        private final Principal principal;

        private Role( String principalId ) {
            this.principal = new Principal( PrincipalType.ROLE, principalId );
        }

        public Principal getPrincipal() {
            return principal;
        }

    };

    private Principals() {}

    public static void ensureUser( Principal principal ) {
        checkState( principal.getType().equals( PrincipalType.USER ), "Only user principal type allowed." );
    }

    /**
     * This will retrieve the current user. If auth information isn't present an NPE is thrown (by design). If the wrong
     * type of auth is present a ClassCast exception will be thrown (by design).
     * 
     * @return The principal for the current request.
     */
    public static @Nonnull Principal getCurrentUser() {
        return getLoomAuthentication().getLoomPrincipal();
    }

    public static Set<Principal> getCurrentPrincipals() {
        return getLoomAuthentication().getLoomPrincipals();
    }
    
    public static LoomAuthentication getLoomAuthentication() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        return ( (LoomAuthentication) auth );
    }

    public static Principal getAdminRole() {
        return Role.ADMIN.getPrincipal();
    }

}
