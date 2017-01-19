package com.dataloom.authorization;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * The authorization manager manages permissions for all securable objects in the system.
 * 
 * Authorization behavior is summarized below:
 * <ul>
 * <li>No inheritance and that all permissions are explicitly set.</li>
 * <li>For permissions that are present we follow a least restrictive model for determining access</li>
 * <li>If no relevant permissions are present for Principal set, access is denied.</li>
 * </ul>
 * 
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 *
 */
public interface AuthorizationManager {

    /**
     * Creates an empty acl.
     * 
     * @param aclKey The key for the object whose acl is being created.
     * @param objectType The type of the object for lookup purposes.
     */
    void createEmptyAcl( List<UUID> aclKey, SecurableObjectType objectType );

    void addPermission(
            List<UUID> aclKeys,
            Principal principal,
            Set<Permission> permissions );

    void removePermission(
            List<UUID> aclKeys,
            Principal principal,
            Set<Permission> permissions );

    void setPermission(
            List<UUID> aclKeys,
            Principal principal,
            Set<Permission> permissions );

    void deletePermissions( List<UUID> aceKey );

    boolean checkIfHasPermissions(
            List<UUID> aclKeys,
            Set<Principal> principals,
            Set<Permission> requiredPermissions );

    boolean checkIfUserIsOwner( List<UUID> aclkeys, Principal principal );
    // Utility functions for retrieving permissions

    Set<Permission> getSecurableObjectPermissions(
            List<UUID> aclKeys,
            Set<Principal> principals );

    Acl getAllSecurableObjectPermissions(
            List<UUID> key );

    Iterable<UUID> getAuthorizedObjectsOfType(
            Principal principal,
            SecurableObjectType objectType,
            EnumSet<Permission> permissions );

    Iterable<UUID> getAuthorizedObjectsOfType(
            Set<Principal> principal,
            SecurableObjectType objectType,
            EnumSet<Permission> permissions );

    Iterable<List<UUID>> getAuthorizedObjects( Principal principal, EnumSet<Permission> permissions );

    Iterable<List<UUID>> getAuthorizedObjects( Set<Principal> principal, EnumSet<Permission> permissions );
}
