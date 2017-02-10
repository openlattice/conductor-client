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
