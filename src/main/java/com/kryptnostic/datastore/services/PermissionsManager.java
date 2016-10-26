package com.kryptnostic.datastore.services;

import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.kryptnostic.datastore.Permission;

public interface PermissionsManager {
    
	// Permissions for a user of an individual type
	void addPermissionsForPropertyType( String role, FullQualifiedName fqn, Set<Permission> permissions );
	
	void removePermissionsForPropertyType( String role, FullQualifiedName fqn, Set<Permission> permissions );

	void setPermissionsForPropertyType( String role, FullQualifiedName fqn, Set<Permission> permissions );

	boolean checkUserHasPermissionsOnPropertyType( Set<String> roles, FullQualifiedName fqn, Permission permission );

	void addPermissionsForEntityType( String role, FullQualifiedName fqn, Set<Permission> permissions );
	
	void removePermissionsForEntityType( String role, FullQualifiedName fqn, Set<Permission> permissions );

	void setPermissionsForEntityType( String role, FullQualifiedName fqn, Set<Permission> permissions );

	boolean checkUserHasPermissionsOnEntityType( Set<String> roles, FullQualifiedName fqn, Permission permission );

	void addPermissionsForEntitySet( String role, FullQualifiedName type, String name, Set<Permission> permissions );

	void removePermissionsForEntitySet( String role, FullQualifiedName type, String name, Set<Permission> permissions );

	void setPermissionsForEntitySet( String role, FullQualifiedName type, String name, Set<Permission> permissions );

	boolean checkUserHasPermissionsOnEntitySet( Set<String> roles, FullQualifiedName type, String name, Permission permission );

	void addPermissionsForSchema( String role, FullQualifiedName fqn, Set<Permission> permissions );			

	void removePermissionsForSchema( String role, FullQualifiedName fqn, Set<Permission> permissions );

	void setPermissionsForSchema( String role, FullQualifiedName fqn, Set<Permission> permissions );

	boolean checkUserHasPermissionsOnSchema( Set<String> roles, FullQualifiedName fqn, Permission permission );

	// Permissions for a user of sub-types
    void addPermissionsForPropertyTypeInEntityType( String role, FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn, Set<Permission> permissions );
    
    void removePermissionsForPropertyTypeInEntityType( String role, FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn, Set<Permission> permissions );

    void setPermissionsForPropertyTypeInEntityType( String role, FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn, Set<Permission> permissions );

    boolean checkUserHasPermissionsOnPropertyTypeInEntityType( Set<String> roles, FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn, Permission permission );

    void addPermissionsForPropertyTypeInEntitySet( String role, FullQualifiedName entityTypeFqn, String entitySetName, FullQualifiedName propertyTypeFqn, Set<Permission> permissions );
    
    void removePermissionsForPropertyTypeInEntitySet( String role, FullQualifiedName entityTypeFqn, String entitySetName, FullQualifiedName propertyTypeFqn, Set<Permission> permissions );

    void setPermissionsForPropertyTypeInEntitySet( String role, FullQualifiedName entityTypeFqn, String entitySetName, FullQualifiedName propertyTypeFqn, Set<Permission> permissions );

    boolean checkUserHasPermissionsOnPropertyTypeInEntitySet( Set<String> roles, FullQualifiedName entityTypeFqn, String entitySetName, FullQualifiedName propertyTypeFqn, Permission permission );

	/**
	 * 	User with ALTER rights to Entity types would automatically have DISCOVER rights to all property types of that entity type
	 *  This is to avoid conflicts when a user tries to add property type to an entity type, which already exists but is unbeknownst to him
	 *  For this reason, need a table to look up users with ALTER rights from entity types
	 */
    void addToEntityTypesAlterRightsTable( String role, FullQualifiedName entityTypeFqn );
    
    void removeFromEntityTypesAlterRightsTable( String role, FullQualifiedName entityTypeFqn );
    
    void addToEntitySetsAlterRightsTable( String role, FullQualifiedName entityTypeFqn, String name );
    
    void removeFromEntitySetsAlterRightsTable( String role, FullQualifiedName entityTypeFqn, String name );

	// Utility functions for removing all permission details associated to a type
	void removePermissionsForPropertyType( FullQualifiedName fqn );

    void removePermissionsForEntityType( FullQualifiedName fqn );

    void removePermissionsForEntitySet( FullQualifiedName entityTypeFqn, String entitySetName );

	void removePermissionsForPropertyTypeInEntityType( FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn );
	
	void removePermissionsForPropertyTypeInEntityType( FullQualifiedName entityTypeFqn );
	
    void removePermissionsForPropertyTypeInEntitySet( FullQualifiedName entityTypeFqn, String entitySetName, FullQualifiedName propertyTypeFqn );

    void removePermissionsForPropertyTypeInEntitySet( FullQualifiedName entityTypeFqn, String entitySetName );

	void removeFromEntityTypesAlterRightsTable( FullQualifiedName entityTypeFqn );
	
    void removeFromEntitySetsAlterRightsTable( FullQualifiedName entityTypeFqn, String name );
    
    // Permission inheritance
    void derivePermissions( FullQualifiedName entityTypeFqn, Set<FullQualifiedName> properties, String derivedOption );

    void inheritPermissionsFromPropertyType( String role, FullQualifiedName entityTypeFqn, Set<FullQualifiedName> properties );

    void inheritPermissionsFromPropertyType( FullQualifiedName entityTypeFqn, Set<FullQualifiedName> properties );

    void inheritPermissionsFromBothTypes( String role, FullQualifiedName entityTypeFqn, Set<FullQualifiedName> properties );

    void inheritPermissionsFromBothTypes( FullQualifiedName entityTypeFqn, Set<FullQualifiedName> properties );
    
}