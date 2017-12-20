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

import com.auth0.spring.security.api.JwtAuthenticationProvider;
import com.dataloom.organizations.roles.SecurePrincipalsManager;
import com.openlattice.auth0.WrappedPreAuthenticatedAuthenticationJwt;
import org.springframework.security.core.Authentication;

public class LoomAuth0AuthenticationProvider extends JwtAuthenticationProvider {
    public static final String USER_ID_ATTRIBUTE    = "user_id";
    public static final String SUBJECT_ATTRIBUTE    = "sub";
    public static final String ISSUE_TIME_ATTRIBUTE = "iat";

    private final SecurePrincipalsManager spm;

    public LoomAuth0AuthenticationProvider(
            byte[] secret,
            String issuer,
            String audience,
            SecurePrincipalsManager spm ) {
        super( secret, issuer, audience );
        this.spm = spm;
    }

    @Override
    public Authentication authenticate( Authentication authentication ) {
        if ( !supports( authentication.getClass() ) ) {
            return null;
        }

        final WrappedPreAuthenticatedAuthenticationJwt jwt = (WrappedPreAuthenticatedAuthenticationJwt) authentication;

        return new LoomAuthentication( super.authenticate( jwt.unwrap() ), spm );
    }

    @Override public boolean supports( Class<?> authentication ) {
        return WrappedPreAuthenticatedAuthenticationJwt.class.isAssignableFrom( authentication );
    }
}
