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

package com.dataloom.requests;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;

public interface PermissionsRequestsManager {
    
    // For users
    void upsertRequest( List<UUID> aclRoot, Principal principal, Map<UUID, EnumSet<Permission>> permissions);

    PermissionsRequest getUnresolvedRequestOfUser( List<UUID> aclRoot, Principal principal );

    Iterable<PermissionsRequest> getResolvedRequestsOfUser( List<UUID> aclRoot, Principal principal );
    
    // For admin
    void updateUnresolvedRequestStatus( List<UUID> aclRoot, Principal principal, RequestStatus status );

    // TODO Pagination
    Iterable<PermissionsRequest> getAllUnresolvedRequestsOfAdmin( List<UUID> aclRoot, EnumSet<RequestStatus> status );
    
    Iterable<PermissionsRequest> getAllUnresolvedRequestsOfAdmin( List<UUID> aclRoot );
    
}
