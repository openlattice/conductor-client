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
            List<AclKey> aclKeys,
            Principal principal,
            Set<Permission> permissions );

    void removePermission(
            List<AclKey> aclKeys,
            Principal principal,
            Set<Permission> permissions );

    void setPermission(
            List<AclKey> aclKeys,
            Principal principal,
            Set<Permission> permissions );

    void deletePermissions( List<AclKey> aceKey );

    boolean checkIfHasPermissions(
            List<AclKey> aclKeys,
            Set<Principal> principals,
            Set<Permission> requiredPermissions );

    boolean checkIfUserIsOwner( List<AclKey> aclkeys, Principal principal );
    // Utility functions for retrieving permissions

    Set<Permission> getSecurableObjectPermissions(
            List<AclKey> aclKeys,
            Set<Principal> principals );

    Acl getAllSecurableObjectPermissions(
            List<AclKey> key );

    Iterable<AclKey> getAuthorizedObjectsOfType(
            Principal principal,
            SecurableObjectType objectType,
            EnumSet<Permission> aces );

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
