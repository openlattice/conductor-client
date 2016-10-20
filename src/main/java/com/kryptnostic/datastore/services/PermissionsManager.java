package com.kryptnostic.datastore.services;

import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.kryptnostic.datastore.Permission;

public interface PermissionsManager {
	
	// Permissions for a user of an individual type
	void addPermissionsForPropertyType( UUID userId, FullQualifiedName fqn, Set<Permission> permissions );
	
	void removePermissionsForPropertyType( UUID userId, FullQualifiedName fqn, Set<Permission> permissions );

	void setPermissionsForPropertyType( UUID userId, FullQualifiedName fqn, Set<Permission> permissions );

	boolean checkUserHasPermissionsOnPropertyType( UUID userId, FullQualifiedName fqn, Permission permission );

	void addPermissionsForEntityType( UUID userId, FullQualifiedName fqn, Set<Permission> permissions );
	
	void removePermissionsForEntityType( UUID userId, FullQualifiedName fqn, Set<Permission> permissions );

	void setPermissionsForEntityType( UUID userId, FullQualifiedName fqn, Set<Permission> permissions );

	boolean checkUserHasPermissionsOnEntityType( UUID userId, FullQualifiedName fqn, Permission permission );

	void addPermissionsForEntitySet( UUID userId, FullQualifiedName type, String name, Set<Permission> permissions );

	void removePermissionsForEntitySet( UUID userId, FullQualifiedName type, String name, Set<Permission> permissions );

	void setPermissionsForEntitySet( UUID userId, FullQualifiedName type, String name, Set<Permission> permissions );

	boolean checkUserHasPermissionsOnEntitySet( UUID userId, FullQualifiedName type, String name, Permission permission );

	void addPermissionsForSchema( UUID userId, FullQualifiedName fqn, Set<Permission> permissions );			

	void removePermissionsForSchema( UUID userId, FullQualifiedName fqn, Set<Permission> permissions );

	void setPermissionsForSchema( UUID userId, FullQualifiedName fqn, Set<Permission> permissions );

	boolean checkUserHasPermissionsOnSchema( UUID userId, FullQualifiedName fqn, Permission permission );

	// Permissions for a user of sub-types
	void addPermissionsForPropertyTypeInEntityType( UUID userId, FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn, Set<Permission> permissions );
	
	void removePermissionsForPropertyTypeInEntityType( UUID userId, FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn, Set<Permission> permissions );

	void setPermissionsForPropertyTypeInEntityType( UUID userId, FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn, Set<Permission> permissions );

	boolean checkUserHasPermissionsOnPropertyTypeInEntityType( UUID userId, FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn, Permission permission );

	/**
	 * 	User with ALTER rights to Entity types would automatically have DISCOVER rights to all property types of that entity type
	 *  This is to avoid conflicts when a user tries to add property type to an entity type, which already exists but is unbeknownst to him
	 *  For this reason, need a table to look up users with ALTER rights from entity types
	 */
	void addToEntityTypesAlterRightsTable( UUID userId, FullQualifiedName entityTypeFqn );
	
	void removeFromEntityTypesAlterRightsTable( UUID userId, FullQualifiedName entityTypeFqn );
	
	// get Permitted Entity Types, Property Types, Entity Sets, Schemas.
	// Variants such as get permitted property type within an entity type, and so on.
	

	// Utility functions for removing all permission details associated to a type
	void removePermissionsForPropertyType( FullQualifiedName fqn );

	void removePermissionsForEntityType( FullQualifiedName fqn );

	void removePermissionsForPropertyTypeInEntityType( FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn );
	
	void removePermissionsForPropertyTypeInEntityType( FullQualifiedName entityTypeFqn );
	
	void removeFromEntityTypesAlterRightsTable( FullQualifiedName entityTypeFqn );

}