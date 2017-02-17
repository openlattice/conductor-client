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

package com.dataloom.requests.mapstores;

import java.util.UUID;

import com.dataloom.authorization.Principal;

public class PrincipalRequestIdPair {
    private UUID              requestId;
    private Principal         user;

    public PrincipalRequestIdPair( Principal user, UUID requestId ) {
        this.user = user;
        this.requestId = requestId;
    }

    public Principal getUser() {
        return user;
    }

    public UUID getRequestId() {
        return requestId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( requestId == null ) ? 0 : requestId.hashCode() );
        result = prime * result + ( ( user == null ) ? 0 : user.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        PrincipalRequestIdPair other = (PrincipalRequestIdPair) obj;
        if ( requestId == null ) {
            if ( other.requestId != null ) return false;
        } else if ( !requestId.equals( other.requestId ) ) return false;
        if ( user == null ) {
            if ( other.user != null ) return false;
        } else if ( !user.equals( other.user ) ) return false;
        return true;
    }

    @Override
    public String toString() {
        return "PrincipalRequestIdPair [principal=" + user + ", requestId=" + requestId + "]";
    }

}
