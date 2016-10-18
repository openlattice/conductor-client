package com.kryptnostic.datastore.services;

import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.kryptnostic.datastore.Permission;

public interface PermissionsManager {
	
	// Permissions for Individual Types
	void addPermissionsForPropertyType( UUID userId, FullQualifiedName fqn, Set<Permission> permissions );
	
	void removePermissionsForPropertyType( UUID userId, FullQualifiedName fqn, Set<Permission> permissions );

	void setPermissionsForPropertyType( UUID userId, FullQualifiedName fqn, Set<Permission> permissions );

	boolean checkUserHasPermissionsOnPropertyType( FullQualifiedName fqn, Permission permission );

	void addPermissionsForEntityType( UUID userId, FullQualifiedName fqn, Set<Permission> permissions );
	
	void removePermissionsForEntityType( UUID userId, FullQualifiedName fqn, Set<Permission> permissions );

	void setPermissionsForEntityType( UUID userId, FullQualifiedName fqn, Set<Permission> permissions );

	boolean checkUserHasPermissionsOnEntityType( FullQualifiedName fqn, Permission permission );

	void addPermissionsForEntitySet( UUID userId, FullQualifiedName type, String name, Set<Permission> permissions );

	void removePermissionsForEntitySet( UUID userId, FullQualifiedName type, String name, Set<Permission> permissions );

	void setPermissionsForEntitySet( UUID userId, FullQualifiedName type, String name, Set<Permission> permissions );

	boolean checkUserHasPermissionsOnEntitySet( FullQualifiedName type, String name, Permission permission );

	void addPermissionsForSchema( UUID userId, FullQualifiedName fqn, Set<Permission> permissions );			

	void removePermissionsForSchema( UUID userId, FullQualifiedName fqn, Set<Permission> permissions );

	void setPermissionsForSchema( UUID userId, FullQualifiedName fqn, Set<Permission> permissions );

	boolean checkUserHasPermissionsOnSchema( FullQualifiedName fqn, Permission permission );

	// Permissions for Sub-types
	void addPermissionsForPropertyTypeInEntityType( UUID userId, FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn, Set<Permission> permissions );
	
	void removePermissionsForPropertyTypeInEntityType( UUID userId, FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn, Set<Permission> permissions );

	void setPermissionsForPropertyTypeInEntityType( UUID userId, FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn, Set<Permission> permissions );

	boolean checkUserHasPermissionsOnPropertyTypeInEntityType( FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn, Permission permission );

	// get Permitted Entity Types, Property Types, Entity Sets, Schemas.
	// Variants such as get permitted property type within an entity type, and so on.
}