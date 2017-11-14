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

import com.dataloom.authorization.paging.AuthorizedObjectsPagingInfo;
import com.dataloom.authorization.paging.AuthorizedObjectsSearchResult;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.openlattice.authorization.AclKey;

import java.util.EnumSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * The authorization manager manages permissions for all securable objects in the system.
 * 
 * Authorization behavior is summarized below:
 * <ul>
 * <li>No inheritance and that all permissions are explicitly set.</li>
 * <li>For permissions that are present we follow a least restrictive model for determining access</li>
 * <li>If no relevant permissions are present for Principal set, access is denied.</li>
 * </ul>
 * 
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 *
 */
public interface AuthorizationManager {

    /**
     * Creates an empty acl.
     * 
     * @param aclKey The key for the object whose acl is being created.
     * @param objectType The type of the object for lookup purposes.
     */
    void createEmptyAcl( AclKey aclKey, SecurableObjectType objectType );

    void addPermission(
            AclKey aclKeys,
            Principal principal,
            Set<Permission> permissions );

    void removePermission(
            AclKey aclKeys,
            Principal principal,
            Set<Permission> permissions );

    void setPermission(
            AclKey aclKeys,
            Principal principal,
            Set<Permission> permissions );

    void deletePermissions( AclKey aclKey );

    void deletePrincipalPermissions( Principal principal );

    boolean checkIfHasPermissions(
            AclKey aclKeys,
            Set<Principal> principals,
            EnumSet<Permission> requiredPermissions );

    boolean checkIfUserIsOwner( AclKey aclkeys, Principal principal );
    // Utility functions for retrieving permissions

    Set<Permission> getSecurableObjectPermissions(
            AclKey aclKeys,
            Set<Principal> principals );

    Acl getAllSecurableObjectPermissions( AclKey key );

    Stream<AclKey> getAuthorizedObjectsOfType(
            Principal principal,
            SecurableObjectType objectType,
            EnumSet<Permission> permissions );

    Stream<AclKey> getAuthorizedObjectsOfType(
            Set<Principal> principal,
            SecurableObjectType objectType,
            EnumSet<Permission> permissions );

    AuthorizedObjectsSearchResult getAuthorizedObjectsOfType(
            NavigableSet<Principal> principals,
            SecurableObjectType objectType,
            Permission permission,
            String offset,
            int pageSize );

    Stream<AclKey> getAuthorizedObjects( Principal principal, EnumSet<Permission> permissions );

    Stream<AclKey> getAuthorizedObjects( Set<Principal> principal, EnumSet<Permission> permissions );
    
    Iterable<Principal> getSecurableObjectOwners( AclKey key );
}
