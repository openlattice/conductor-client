package com.dataloom.authorization;

import static com.google.common.base.Preconditions.checkState;

import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.context.SecurityContextHolder;

import com.auth0.spring.security.api.Auth0UserDetails;
import com.google.common.collect.Sets;

public final class Principals {
    private static final Logger logger            = LoggerFactory.getLogger( Principals.class );
    private static final String USER_ID_ATTRIBUTE = "user_id";
    private static final String SUBJECT_ATTRIBUTE = "sub";

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

    private static final ThreadLocal<Set<Principal>> currentPrincipalsCache = new ThreadLocal<Set<Principal>>() {
        protected Set<Principal> initialValue() {
            return SecurityContextHolder.getContext().getAuthentication().getAuthorities().stream()
                    .map( authority -> new Principal( PrincipalType.ROLE, authority.getAuthority() ) )
                    .collect( Collectors.toCollection( () -> Sets.newHashSet( getCurrentUser() ) ) );

        }
    };

    public static void ensureUser( Principal principal ) {
        checkState( principal.getType().equals( PrincipalType.USER ), "Only user principal type allowed." );
    }

    public static Principal getCurrentUser() {
        Object principal = SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        final Auth0UserDetails details;

        if ( principal != null && Auth0UserDetails.class.isAssignableFrom( principal.getClass() ) ) {
            details = (Auth0UserDetails) principal;
        } else {
            if ( principal != null ) {
                logger.error( "Encountered unexpected principal: {}", principal );
            }
            throw new ForbiddenException( "No authentication found when authentication expected" );
        }

        Object principalId = details.getAuth0Attribute( SUBJECT_ATTRIBUTE );

        if ( principalId == null ) {
            principalId = details.getAuth0Attribute( USER_ID_ATTRIBUTE );
        }

        return new Principal(
                PrincipalType.USER,
                principalId.toString() );
    }

    public static Set<Principal> getCurrentPrincipals() {
        return currentPrincipalsCache.get();
    }

    public static Principal getAdminRole() {
        return Role.ADMIN.getPrincipal();
    }

}
