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

import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.authorization.securable.AbstractSecurableObject;
import com.google.common.collect.ImmutableList;

public interface AuthorizingComponent {
    AuthorizationManager getAuthorizationManager();

    default <T extends AbstractSecurableObject> Predicate<T> isAuthorizedObject(
            Permission requiredPermission,
            Permission... requiredPermissions ) {
        return abs -> isAuthorized( requiredPermission, requiredPermissions )
                .test( ImmutableList.of( abs.getId() ) );
    }

    default Predicate<List<UUID>> isAuthorized(
            Permission requiredPermission,
            Permission... requiredPermissions ) {
        return isAuthorized( EnumSet.of( requiredPermission, requiredPermissions ) );
    }

    default Predicate<List<UUID>> isAuthorized( EnumSet<Permission> requiredPermissions ) {
        return aclKey -> getAuthorizationManager().checkIfHasPermissions( aclKey,
                Principals.getCurrentPrincipals(),
                requiredPermissions );
    }

    default boolean owns( List<UUID> aclKey ) {
        return isAuthorized( Permission.OWNER ).test( aclKey );
    }

    default void ensureReadAccess( List<UUID> aclKey ) {
        accessCheck( aclKey, EnumSet.of( Permission.READ ) );
    }

    default void ensureWriteAccess( List<UUID> aclKey ) {
        accessCheck( aclKey, EnumSet.of( Permission.WRITE ) );
    }

    default void ensureOwnerAccess( List<UUID> aclKey ) {
        accessCheck( aclKey, EnumSet.of( Permission.OWNER ) );
    }

    default void ensureAdminAccess() {
        if ( !Principals.getCurrentPrincipals().contains( Principals.getAdminRole() ) ) {
            throw new ForbiddenException( "Only admins are allowed to perform this action." );
        }
    }

    default void accessCheck( List<UUID> aclKey, EnumSet<Permission> requiredPermissions ) {
        if ( !getAuthorizationManager().checkIfHasPermissions(
                aclKey,
                Principals.getCurrentPrincipals(),
                requiredPermissions ) ) {
            throw new ForbiddenException( "Object " + aclKey.toString() + " is not accessible." );
        }
    }

    default Stream<List<UUID>> getAccessibleObjects(
            SecurableObjectType securableObjectType,
            EnumSet<Permission> requiredPermissions ) {
        return getAuthorizationManager().getAuthorizedObjectsOfType(
                Principals.getCurrentPrincipals(),
                securableObjectType,
                requiredPermissions );
    }
}
