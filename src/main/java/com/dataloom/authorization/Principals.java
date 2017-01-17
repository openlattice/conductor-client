package com.dataloom.authorization;

import static com.google.common.base.Preconditions.checkState;

import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.security.core.context.SecurityContextHolder;

import com.auth0.spring.security.api.Auth0UserDetails;
import com.google.common.collect.Sets;

public final class Principals {
    private static final String USER_ID_ATTRIBUTE = "user_id";
    private static final String SUBJECT_ATTRIBUTE = "sub";

    private Principals() {}

    private static final ThreadLocal<Set<Principal>> currentPrincipalsCache = new ThreadLocal<Set<Principal>>() {
        protected Set<Principal> initialValue() {
            return SecurityContextHolder.getContext().getAuthentication().getAuthorities().stream()
                    .map( authority -> new Principal( PrincipalType.ROLE, authority.getAuthority() ) )
                    .collect( Collectors.toCollection( () -> Sets.newHashSet( getCurrentUser() ) ) );

        };
    };

    public static void ensureUser( Principal principal ) {
        checkState( principal.getType().equals( PrincipalType.USER ), "Only user principal type allowed." );
    }

    public static Principal getCurrentUser() {
        Auth0UserDetails details = (Auth0UserDetails) SecurityContextHolder.getContext().getAuthentication()
                .getPrincipal();
        Object principalId = details.getAuth0Attribute( USER_ID_ATTRIBUTE );
        if ( principalId == null ) {
            principalId = details.getAuth0Attribute( SUBJECT_ATTRIBUTE );
        }

        return new Principal(
                PrincipalType.USER,
                principalId.toString() );
    }

    public static Set<Principal> getCurrentPrincipals() {
        return currentPrincipalsCache.get();
    }

}
