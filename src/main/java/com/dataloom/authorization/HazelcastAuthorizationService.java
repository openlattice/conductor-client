package com.dataloom.authorization;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.authorization.processors.PermissionMerger;
import com.dataloom.authorization.processors.PermissionRemover;
import com.dataloom.authorization.requests.Permission;
import com.dataloom.authorization.requests.PermissionsInfo;
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
            SecurableObjectType objectType,
            UUID objectId,
            Principal principal,
            Set<Permission> permissions ) {
        aces.executeOnKey( new AceKey( objectId, objectType, principal ), new PermissionMerger( permissions ) );
    }

    @Override
    public void removePermission(
            SecurableObjectType objectType,
            UUID objectId,
            Principal principal,
            Set<Permission> permissions ) {
        aces.executeOnKey( new AceKey( objectId, objectType, principal ), new PermissionRemover( permissions ) );
    }

    @Override
    public void setPermission(
            SecurableObjectType objectType,
            UUID objectId,
            Principal principal,
            Set<Permission> permissions ) {
        aces.set( new AceKey( objectId, objectType, principal ), permissions );
    }

    @Override
    public boolean checkIfHasPermissions(
            SecurableObjectType objectType,
            UUID objectId,
            Set<Principal> principals,
            FullQualifiedName fqn,
            Permission permission ) {

        Set<Permission> permissions = getSecurableObjectPermissions( objectType, objectId, principals );
        return permissions.contains( permission );
    }

    @Override
    public boolean checkIfUserIsOwner( SecurableObjectType objectType, UUID objectId, Principal principal ) {
        checkArgument( principal.getType().equals( PrincipalType.USER ), "A role cannot be the owner of an object" );
        // TODO Consider using owner permission
        return false;
    }

    @Override
    public Set<Permission> getSecurableObjectPermissions(
            SecurableObjectType objectType,
            UUID objectId,
            Set<Principal> principals ) {
        return aces
                .getAll( principals.stream().map( principal -> new AceKey( objectId, objectType, principal ) )
                        .collect( Collectors.toSet() ) )
                .values().stream().flatMap( permissions -> permissions.stream() )
                .collect( Collectors.toCollection( () -> EnumSet.noneOf( Permission.class ) ) );

    }

    @Override
    public Map<FullQualifiedName, EnumSet<Permission>> getPropertyTypesInEntitySetAcls(
            Set<Principal> principals,
            String entitySetName ) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public EnumSet<Permission> getEntityTypeAclsForUser(
            String username,
            List<String> currentRoles,
            FullQualifiedName entityTypeFqn ) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<FullQualifiedName, EnumSet<Permission>> getPropertyTypesInEntityTypeAclsForUser(
            String username,
            List<String> currentRoles,
            FullQualifiedName entityTypeFqn ) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Iterable<PermissionsInfo> getEntitySetAclsForOwner( String entitySetName ) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Iterable<PermissionsInfo> getPropertyTypesInEntitySetAclsForOwner(
            String entitySetName,
            FullQualifiedName propertyTypeFqn ) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<FullQualifiedName, EnumSet<Permission>> getPropertyTypesInEntitySetAclsOfPrincipalForOwner(
            String entitySetName,
            Principal principal ) {
        // TODO Auto-generated method stub
        return null;
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
