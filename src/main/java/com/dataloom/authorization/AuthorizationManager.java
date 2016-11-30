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

public interface AuthorizationManager {

    void addPermission(
            SecurableObjectType objectType,
            UUID objectId,
            Principal principal,
            Set<Permission> permissions );

    void removePermission(
            SecurableObjectType objectType,
            UUID objectId,
            Principal principal,
            Set<Permission> permissions );

    void setPermission(
            SecurableObjectType objectType,
            UUID objectId,
            Principal principal,
            Set<Permission> permissions );

    boolean checkIfHasPermissions(
            SecurableObjectType objectType,
            UUID objectId,
            Set<String> principals,
            FullQualifiedName fqn,
            Permission permission );

    boolean checkIfUserIsOwner( SecurableObjectType objectType, UUID objectId, String username );

    // Utility functions for retrieving permissions

    EnumSet<Permission> getSecurableObjectPermissiosn(
            SecurableObjectType objectType,
            String userId,
            String objectId,
            Set<String> currentRoles );

    Map<FullQualifiedName, EnumSet<Permission>> getPropertyTypesInEntitySetAclsForUser(
            String username,
            List<String> currentRoles,
            String entitySetName );

    EnumSet<Permission> getEntityTypeAclsForUser(
            String username,
            List<String> currentRoles,
            FullQualifiedName entityTypeFqn );

    Map<FullQualifiedName, EnumSet<Permission>> getPropertyTypesInEntityTypeAclsForUser(
            String username,
            List<String> currentRoles,
            FullQualifiedName entityTypeFqn );

    Iterable<PermissionsInfo> getEntitySetAclsForOwner( String entitySetName );

    Iterable<PermissionsInfo> getPropertyTypesInEntitySetAclsForOwner(
            String entitySetName,
            FullQualifiedName propertyTypeFqn );

    Map<FullQualifiedName, EnumSet<Permission>> getPropertyTypesInEntitySetAclsOfPrincipalForOwner(
            String entitySetName,
            Principal principal );

    // Methods for requesting permissions

    void addPermissionsRequestForPropertyTypeInEntitySet(
            String username,
            Principal principal,
            String entitySetName,
            FullQualifiedName propertyTypeFqn,
            EnumSet<Permission> permissions );

    void removePermissionsRequestForEntitySet( UUID id );

    Iterable<PropertyTypeInEntitySetAclRequest> getAllReceivedRequestsForPermissionsOfUsername( String username );

    Iterable<PropertyTypeInEntitySetAclRequest> getAllReceivedRequestsForPermissionsOfEntitySet( String entitySetName );

    Iterable<PropertyTypeInEntitySetAclRequest> getAllSentRequestsForPermissions( String username );

    Iterable<PropertyTypeInEntitySetAclRequest> getAllSentRequestsForPermissions(
            String username,
            String entitySetName );

}
