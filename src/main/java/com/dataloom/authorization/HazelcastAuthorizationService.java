package com.dataloom.authorization;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.authorization.processors.PermissionMerger;
import com.dataloom.authorization.processors.PermissionRemover;
import com.dataloom.authorization.requests.Permission;
import com.dataloom.authorization.requests.Principal;
import com.dataloom.authorization.requests.PrincipalType;
import com.dataloom.edm.requests.PropertyTypeInEntitySetAclRequest;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class HazelcastAuthorizationService implements AuthorizationManager {
    public static final String                  ACES_MAP = "aces";

    private final IMap<AceKey, Set<Permission>> aces;
    private final AuthorizationQueryService     aqs;

    public HazelcastAuthorizationService( HazelcastInstance hazelcastInstance, AuthorizationQueryService aqs ) {
        aces = hazelcastInstance.getMap( ACES_MAP );
        this.aqs = checkNotNull( aqs );
    }

    @Override
    public void addPermission(
            List<AclKey> key,
            Principal principal,
            Set<Permission> permissions ) {
        aces.executeOnKey( new AceKey( key, principal ), new PermissionMerger( permissions ) );
    }

    @Override
    public void removePermission(
            List<AclKey> key,
            Principal principal,
            Set<Permission> permissions ) {
        aces.executeOnKey( new AceKey( key, principal ), new PermissionRemover( permissions ) );
    }

    @Override
    public void setPermission(
            List<AclKey> key,
            Principal principal,
            Set<Permission> permissions ) {
        aces.set( new AceKey( key, principal ), permissions );
    }

    @Override
    public boolean checkIfHasPermissions(
            List<AclKey> key,
            Set<Principal> principals,
            Set<Permission> requiredPermissions ) {
        Set<Permission> permissions = getSecurableObjectPermissions( key, principals );
        return permissions.containsAll( requiredPermissions );
    }

    @Override
    public boolean checkIfUserIsOwner( List<AclKey> aclKeys, Principal principal ) {
        checkArgument( principal.getType().equals( PrincipalType.USER ), "A role cannot be the owner of an object" );
        // TODO Consider using owner permission
        return false;
    }

    @Override
    public Set<Permission> getSecurableObjectPermissions(
            List<AclKey> key,
            Set<Principal> principals ) {
        return aces
                .getAll( principals.stream().map( principal -> new AceKey( key, principal ) )
                        .collect( Collectors.toSet() ) )
                .values().stream().flatMap( permissions -> permissions.stream() )
                .collect( Collectors.toCollection( () -> EnumSet.noneOf( Permission.class ) ) );

    }

    @Override
    public Acl getAllSecurableObjectPermissions( List<AclKey> key ) {
        return aqs.getAclsForSecurableObject( key );
    }

    @Override
    public void addPermissionsRequestForPropertyTypeInEntitySet(
            String username,
            Principal principal,
            String entitySetName,
            FullQualifiedName propertyTypeFqn,
            EnumSet<Permission> permissions ) {
        // TODO Auto-generated method stub

    }

    @Override
    public void removePermissionsRequestForEntitySet( UUID id ) {
        // TODO Auto-generated method stub

    }

    @Override
    public Iterable<PropertyTypeInEntitySetAclRequest> getAllReceivedRequestsForPermissionsOfUsername(
            String username ) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Iterable<PropertyTypeInEntitySetAclRequest> getAllReceivedRequestsForPermissionsOfEntitySet(
            String entitySetName ) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Iterable<PropertyTypeInEntitySetAclRequest> getAllSentRequestsForPermissions( String username ) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Iterable<PropertyTypeInEntitySetAclRequest> getAllSentRequestsForPermissions(
            String username,
            String entitySetName ) {
        // TODO Auto-generated method stub
        return null;
    }

}
