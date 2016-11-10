package com.kryptnostic.datastore.services;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.web.bind.annotation.RequestParam;

import com.kryptnostic.datastore.Permission;
import com.kryptnostic.datastore.PermissionsInfo;
import com.kryptnostic.datastore.Principal;
import com.kryptnostic.datastore.services.requests.PropertyTypeInEntitySetAclRequest;

public interface PermissionsManager {
    
    /**
     * Permissions for a user of an individual type
     */
    void addPermissionsForEntityType( Principal principal, FullQualifiedName fqn, Set<Permission> permissions );
	
	void removePermissionsForEntityType( Principal principal, FullQualifiedName fqn, Set<Permission> permissions );

	void setPermissionsForEntityType( Principal principal, FullQualifiedName fqn, Set<Permission> permissions );

    boolean checkUserHasPermissionsOnEntityType( String username, List<String> roles, FullQualifiedName fqn, Permission permission );

	void addPermissionsForEntitySet( Principal principal, String name, Set<Permission> permissions );

	void removePermissionsForEntitySet( Principal principal, String name, Set<Permission> permissions );

	void setPermissionsForEntitySet( Principal principal, String name, Set<Permission> permissions );

    boolean checkUserHasPermissionsOnEntitySet( String username, List<String> roles, String name, Permission permission );
    
    boolean checkIfUserIsOwnerOfEntitySet( String username, String name );
    
    boolean checkIfUserIsOwnerOfEntitySet( String username, UUID requestId );
    
    boolean checkIfUserIsOwnerOfPermissionsRequest( String username, UUID requestId );

	// Permissions for a user of pair of types
    void addPermissionsForPropertyTypeInEntityType( Principal principal, FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn, Set<Permission> permissions );
    
    void removePermissionsForPropertyTypeInEntityType( Principal principal, FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn, Set<Permission> permissions );

    void setPermissionsForPropertyTypeInEntityType( Principal principal, FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn, Set<Permission> permissions );

    boolean checkUserHasPermissionsOnPropertyTypeInEntityType( String username, List<String> roles, FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn, Permission permission );

    void addPermissionsForPropertyTypeInEntitySet( Principal principal, String entitySetName, FullQualifiedName propertyTypeFqn, Set<Permission> permissions );
    
    void removePermissionsForPropertyTypeInEntitySet( Principal principal, String entitySetName, FullQualifiedName propertyTypeFqn, Set<Permission> permissions );

    void setPermissionsForPropertyTypeInEntitySet( Principal principal, String entitySetName, FullQualifiedName propertyTypeFqn, Set<Permission> permissions );

    boolean checkUserHasPermissionsOnPropertyTypeInEntitySet( String username, List<String> roles, String entitySetName, FullQualifiedName propertyTypeFqn, Permission permission );

	// Utility functions for removing all permission details associated to a type

    void removePermissionsForEntityType( FullQualifiedName fqn );

    void removePermissionsForEntitySet( String entitySetName );

	void removePermissionsForPropertyTypeInEntityType( FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn );
	
	void removePermissionsForPropertyTypeInEntityType( FullQualifiedName entityTypeFqn );
	
    void removePermissionsForPropertyTypeInEntitySet( String entitySetName, FullQualifiedName propertyTypeFqn );

    void removePermissionsForPropertyTypeInEntitySet( String entitySetName );
    
    // Utility functions for retrieving permissions

    EnumSet<Permission> getEntitySetAclsForUser( String username, List<String> currentRoles, String entitySetName );

    Map<FullQualifiedName, EnumSet<Permission>> getPropertyTypesInEntitySetAclsForUser( String username, List<String> currentRoles, String entitySetName );

    EnumSet<Permission> getEntityTypeAclsForUser( String username, List<String> currentRoles, FullQualifiedName entityTypeFqn );
    
    Map<FullQualifiedName, EnumSet<Permission>> getPropertyTypesInEntityTypeAclsForUser( String username, List<String> currentRoles, FullQualifiedName entityTypeFqn );

    Iterable<PermissionsInfo> getEntitySetAclsForOwner( String entitySetName );
    
    Map<FullQualifiedName, EnumSet<Permission>> getPropertyTypesInEntitySetAclsForOwner( String entitySetName, Principal principal );
    
    // Methods for requesting permissions
    
    void addPermissionsRequestForPropertyTypeInEntitySet( String username, Principal principal, String entitySetName, FullQualifiedName propertyTypeFqn, EnumSet<Permission> permissions );

    void removePermissionsRequestForEntitySet( UUID id );
    
    Iterable<PropertyTypeInEntitySetAclRequest> getAllReceivedRequestsForPermissionsOfUsername( String username );
    
    Iterable<PropertyTypeInEntitySetAclRequest> getAllReceivedRequestsForPermissionsOfEntitySet( String entitySetName );
    
    Iterable<PropertyTypeInEntitySetAclRequest> getAllSentRequestsForPermissions( String username );
    
    Iterable<PropertyTypeInEntitySetAclRequest> getAllSentRequestsForPermissions( String username, String entitySetName );

}