package com.kryptnostic.datastore.services;

import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.kryptnostic.datastore.Permission;

public interface PermissionsManager {
	
	void addPermissionsForPropertyTypes( UUID userId, FullQualifiedName fqn, Set<Permission> permissions );
	
	void removePermissionsForPropertyTypes( UUID userId, FullQualifiedName fqn, Set<Permission> permissions );

	void setPermissionsForPropertyTypes( UUID userId, FullQualifiedName fqn, Set<Permission> permissions );

	boolean checkUserHasPermissionsOnPropertyType( FullQualifiedName fqn, Permission permission );

	void addPermissionsForEntityTypes( UUID userId, FullQualifiedName fqn, Set<Permission> permissions );
	
	void removePermissionsForEntityTypes( UUID userId, FullQualifiedName fqn, Set<Permission> permissions );

	void setPermissionsForEntityTypes( UUID userId, FullQualifiedName fqn, Set<Permission> permissions );

	boolean checkUserHasPermissionsOnEntityType( FullQualifiedName fqn, Permission permission );

	void addPermissionsForEntitySets( UUID userId, FullQualifiedName type, String name, Set<Permission> permissions );

	void removePermissionsForEntitySets( UUID userId, FullQualifiedName type, String name, Set<Permission> permissions );

	void setPermissionsForEntitySets( UUID userId, FullQualifiedName type, String name, Set<Permission> permissions );

	boolean checkUserHasPermissionsOnEntitySet( FullQualifiedName type, String name, Permission permission );

	void addPermissionsForSchemas( UUID userId, FullQualifiedName fqn, Set<Permission> permissions );			

	void removePermissionsForSchemas( UUID userId, FullQualifiedName fqn, Set<Permission> permissions );

	void setPermissionsForSchemas( UUID userId, FullQualifiedName fqn, Set<Permission> permissions );

	boolean checkUserHasPermissionsOnSchema( FullQualifiedName fqn, Permission permission );

	// get Permitted Entity Types, Property Types, Entity Sets, Schemas.
	// Variants such as get permitted property type within an entity type, and so on.
}