package com.dataloom.authorization;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import com.dataloom.edm.internal.AbstractSecurableObject;
import com.google.common.collect.ImmutableList;

public interface AuthorizingComponent {
    AuthorizationManager getAuthorizationManager();

    default <T extends AbstractSecurableObject> Predicate<T> isAuthorizedObject(
            Permission requiredPermission,
            Permission... requiredPermissions ) {
        return abs -> isAuthorized( requiredPermission, requiredPermissions )
                .test( ImmutableList.of( abs.getAclKeyPathFragment() ) );
    }

    default Predicate<List<AclKeyPathFragment>> isAuthorized(
            Permission requiredPermission,
            Permission... requiredPermissions ) {
        return isAuthorized( EnumSet.of( requiredPermission, requiredPermissions ) );
    }

    default Predicate<List<AclKeyPathFragment>> isAuthorized( Set<Permission> requiredPermissions ) {
        return aclKey -> getAuthorizationManager().checkIfHasPermissions( aclKey,
                Principals.getCurrentPrincipals(),
                requiredPermissions );
    }

    default void ensureReadAccess( List<AclKeyPathFragment> aclKey ) {
        accessCheck( aclKey, EnumSet.of( Permission.READ ) );
    }

    default void ensureWriteAccess( List<AclKeyPathFragment> aclKey ) {
        accessCheck( aclKey, EnumSet.of( Permission.WRITE ) );
    }

    default void accessCheck( List<AclKeyPathFragment> aclKey, Set<Permission> requiredPermissions ) {
        if ( !getAuthorizationManager().checkIfHasPermissions(
                aclKey,
                Principals.getCurrentPrincipals(),
                requiredPermissions ) ) {
            throw new ForbiddenException( "Object " + aclKey.toString() + " is not accessible." );
        }
    }
}
