package com.kryptnostic.datastore.services;

import java.util.List;
import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.kryptnostic.datastore.Permission;

public interface PermissionsManager {
    
    /**
     * Permissions for a user of an individual type
     */
	void addPermissionsForEntityType( String role, FullQualifiedName fqn, Set<Permission> permissions );
	
	void removePermissionsForEntityType( String role, FullQualifiedName fqn, Set<Permission> permissions );

	void setPermissionsForEntityType( String role, FullQualifiedName fqn, Set<Permission> permissions );

    boolean checkUserHasPermissionsOnEntityType( List<String> roles, FullQualifiedName fqn, Permission permission );

	void addPermissionsForEntitySet( String role, FullQualifiedName type, String name, Set<Permission> permissions );

	void removePermissionsForEntitySet( String role, FullQualifiedName type, String name, Set<Permission> permissions );

	void setPermissionsForEntitySet( String role, FullQualifiedName type, String name, Set<Permission> permissions );

	boolean checkUserHasPermissionsOnEntitySet( List<String> roles, FullQualifiedName type, String name, Permission permission );

	// Permissions for a user of pair of types
    void addPermissionsForPropertyTypeInEntityType( String role, FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn, Set<Permission> permissions );
    
    void removePermissionsForPropertyTypeInEntityType( String role, FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn, Set<Permission> permissions );

    void setPermissionsForPropertyTypeInEntityType( String role, FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn, Set<Permission> permissions );

    boolean checkUserHasPermissionsOnPropertyTypeInEntityType( List<String> roles, FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn, Permission permission );

    void addPermissionsForPropertyTypeInEntitySet( String role, FullQualifiedName entityTypeFqn, String entitySetName, FullQualifiedName propertyTypeFqn, Set<Permission> permissions );
    
    void removePermissionsForPropertyTypeInEntitySet( String role, FullQualifiedName entityTypeFqn, String entitySetName, FullQualifiedName propertyTypeFqn, Set<Permission> permissions );

    void setPermissionsForPropertyTypeInEntitySet( String role, FullQualifiedName entityTypeFqn, String entitySetName, FullQualifiedName propertyTypeFqn, Set<Permission> permissions );

    boolean checkUserHasPermissionsOnPropertyTypeInEntitySet( List<String> roles, FullQualifiedName entityTypeFqn, String entitySetName, FullQualifiedName propertyTypeFqn, Permission permission );

	// Utility functions for removing all permission details associated to a type

    void removePermissionsForEntityType( FullQualifiedName fqn );

    void removePermissionsForEntitySet( FullQualifiedName entityTypeFqn, String entitySetName );

	void removePermissionsForPropertyTypeInEntityType( FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn );
	
	void removePermissionsForPropertyTypeInEntityType( FullQualifiedName entityTypeFqn );
	
    void removePermissionsForPropertyTypeInEntitySet( FullQualifiedName entityTypeFqn, String entitySetName, FullQualifiedName propertyTypeFqn );

    void removePermissionsForPropertyTypeInEntitySet( FullQualifiedName entityTypeFqn, String entitySetName );

}