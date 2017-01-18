package com.dataloom.authorization;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;

import com.dataloom.edm.internal.AbstractSecurableObject;
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

    default Predicate<List<UUID>> isAuthorized( Set<Permission> requiredPermissions ) {
        return aclKey -> getAuthorizationManager().checkIfHasPermissions( aclKey,
                Principals.getCurrentPrincipals(),
                requiredPermissions );
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

    default void accessCheck( List<UUID> aclKey, Set<Permission> requiredPermissions ) {
        if ( !getAuthorizationManager().checkIfHasPermissions(
                aclKey,
                Principals.getCurrentPrincipals(),
                requiredPermissions ) ) {
            throw new ForbiddenException( "Object " + aclKey.toString() + " is not accessible." );
        }
    }

    default Iterable<UUID> getAccessibleObjects(
            SecurableObjectType securableObjectType,
            EnumSet<Permission> requiredPermissions ) {
        return getAuthorizationManager().getAuthorizedObjectsOfType(
                Principals.getCurrentPrincipals(),
                securableObjectType,
                requiredPermissions );
    }
}
