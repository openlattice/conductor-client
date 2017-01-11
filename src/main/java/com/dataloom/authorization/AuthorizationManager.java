package com.dataloom.authorization;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.edm.exceptions.AclKeyConflictException;

/**
 * The authorization manager manages permissions for all securable objects in the system.
 * 
 * Authorization behavior is summarized below: 
 * <ul>
 * <li> No inheritance and that all permissions are explicitly set.</li>
 * <li> For permissions that are present we follow a least restrictive model for determining access </li>
 * <li> If no relevant permissions are present for Principal set, access is denied.   </li>
 * </ul>
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 *
 */
public interface AuthorizationManager {

    /**
     * This is a high performance version of {@link AuthorizationManager#reserveOwnershipIfNotExists(List, Principal)}
     * that does not throw and instead returns a boolean status. Prefer this method for tight performance loops were
     * exceptions can wreck perf. Unless debug is enabled, reasons for failure will be swallowed.
     * 
     * @param aclKeys The list of aclKeys that uniquely identify the object in the hierarchy.
     * @param principal The user for whom to reserve ownership.
     * @return True if reservations was successful, false otherwise.
     */
//    boolean tryReserveOwnershipIfNotExists( List<AclKey> aclKeys, Principal principal );

    /**
     * Reserves a permission entry with the specified principal as owner.
     * 
     * @param aclKeys The list of aclKeys that uniquely identify the object in the hierarchy.
     * @param principal The user for whom to reserve ownership.
     * @throws AclKeyConflictException If this aclKey has already been reserved.
     */
//    void reserveOwnershipIfNotExists( List<AclKey> aclKeys, Principal principal ) throws AclKeyConflictException;
    void addPermission(
            List<AclKeyPathFragment> aclKeys,
            Principal principal,
            Set<Permission> permissions );

    void removePermission(
            List<AclKeyPathFragment> aclKeys,
            Principal principal,
            Set<Permission> permissions );

    void setPermission(
            List<AclKeyPathFragment> aclKeys,
            Principal principal,
            Set<Permission> permissions );

    void deletePermissions( List<AclKeyPathFragment> aceKey );

    boolean checkIfHasPermissions(
            List<AclKeyPathFragment> aclKeys,
            Set<Principal> principals,
            Set<Permission> requiredPermissions );

    boolean checkIfUserIsOwner( List<AclKeyPathFragment> aclkeys, Principal principal );
    // Utility functions for retrieving permissions

    Set<Permission> getSecurableObjectPermissions(
            List<AclKeyPathFragment> aclKeys,
            Set<Principal> principals );

    Acl getAllSecurableObjectPermissions(
            List<AclKeyPathFragment> key );

    Iterable<AclKeyPathFragment> getAuthorizedObjectsOfType(
            Principal principal,
            SecurableObjectType objectType,
            EnumSet<Permission> permissions );

    Iterable<AclKeyPathFragment> getAuthorizedObjectsOfType(
            Set<Principal> principal,
            SecurableObjectType objectType,
            EnumSet<Permission> permissions );
    
    Map<String, AclKeyInfo> getAuthorizedObjects( Map<Principal, EnumSet<Permission>> aces );

    // Methods for requesting permissions

    void addPermissionsRequestForPropertyTypeInEntitySet(
            String username,
            Principal principal,
            String entitySetName,
            FullQualifiedName propertyTypeFqn,
            EnumSet<Permission> permissions );

    void removePermissionsRequestForEntitySet( UUID id );

}
