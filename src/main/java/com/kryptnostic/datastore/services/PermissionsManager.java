package com.kryptnostic.datastore.services;

import java.util.List;
import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.kryptnostic.datastore.Permission;
import com.kryptnostic.datastore.Principal;

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

}