package com.dataloom.authorization;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.authorization.requests.Permission;
import com.dataloom.authorization.requests.Principal;
import com.dataloom.edm.requests.PropertyTypeInEntitySetAclRequest;

public interface AuthorizationManager {

    void addPermission(
            List<AclKey> aclKey,
            Principal principal,
            Set<Permission> permissions );

    void removePermission(
            List<AclKey> aclKey,
            Principal principal,
            Set<Permission> permissions );

    void setPermission(
            List<AclKey> aclKey,
            Principal principal,
            Set<Permission> permissions );

    boolean checkIfHasPermissions(
            List<AclKey> aclKey,
            Set<Principal> principals,
            Set<Permission> requiredPermissions );

    boolean checkIfUserIsOwner( List<AclKey> aclKeys, Principal principal );

    // Utility functions for retrieving permissions

    Set<Permission> getSecurableObjectPermissions(
            List<AclKey> aclKey,
            Set<Principal> principals );

    Iterable<Ace> getAllSecurableObjectPermissions(
            List<AclKey> key );
    
    Map<String, AclKeyInfo> getAuthorizedObjects( Map<Principal, EnumSet<Permission>> aces );
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
