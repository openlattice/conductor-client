package com.dataloom.authorization;

import static com.google.common.base.Preconditions.checkState;

import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.security.core.context.SecurityContextHolder;

import com.auth0.spring.security.api.Auth0UserDetails;
import com.google.common.collect.Sets;

public final class Principals {
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
        return new Principal(
                PrincipalType.USER,
                ( (Auth0UserDetails) SecurityContextHolder.getContext().getAuthentication().getPrincipal() )
                        .getUsername() );
    }

    public static Set<Principal> getCurrentPrincipals() {
        return currentPrincipalsCache.get();
    }

}
