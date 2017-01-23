package com.dataloom.authorization;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Stream;

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

    default Stream<UUID> getAccessibleObjects(
            SecurableObjectType securableObjectType,
            EnumSet<Permission> requiredPermissions ) {
        return getAuthorizationManager().getAuthorizedObjectsOfType(
                Principals.getCurrentPrincipals(),
                securableObjectType,
                requiredPermissions );
    }
}
