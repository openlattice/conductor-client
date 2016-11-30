package com.dataloom.authorization;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.authorization.requests.Permission;
import com.dataloom.authorization.requests.PermissionsInfo;
import com.dataloom.authorization.requests.Principal;
import com.dataloom.edm.requests.PropertyTypeInEntitySetAclRequest;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class HazelcastAuthorizationService implements AuthorizationManager {
    public static final String                                 ACES_MAP = "aces";

    private final IMap<AceKey, Set<Permission>> aces;

    public HazelcastAuthorizationService( HazelcastInstance hazelcastInstance ) {
        aces = hazelcastInstance.getMap( ACES_MAP );
    }

    @Override
    public void addPermission(
            SecurableObjectType objectType,
            UUID objectId,
            Principal principal,
            Set<Permission> permissions ) {
        //TODO: Use abstract merger
        aces.put( new AceKey( objectId, objectType, principal ), permissions );
    }

    @Override
    public void removePermission(
            SecurableObjectType objectType,
            UUID objectId,
            Principal principal,
            Set<Permission> permissions ) {
        aces.delete( new AceKey( objectId, objectType, principal ) );
    }

    @Override
    public void setPermission(
            SecurableObjectType objectType,
            UUID objectId,
            Principal principal,
            Set<Permission> permissions ) {
        aces.put( new AceKey( objectId, objectType, principal ), permissions );
    }

    @Override
    public boolean checkIfHasPermissions(
            SecurableObjectType objectType,
            UUID objectId,
            Set<String> principals,
            FullQualifiedName fqn,
            Permission permission ) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean checkIfUserIsOwner( SecurableObjectType objectType, UUID objectId, String username ) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public EnumSet<Permission> getSecurableObjectPermissiosn(
            SecurableObjectType objectType,
            String userId,
            String objectId,
            Set<String> currentRoles ) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<FullQualifiedName, EnumSet<Permission>> getPropertyTypesInEntitySetAclsForUser(
            String username,
            List<String> currentRoles,
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
