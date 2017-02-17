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
