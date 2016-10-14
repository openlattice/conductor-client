package com.kryptnostic.datastore.services;

import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.datastax.driver.core.Session;
import com.kryptnostic.datastore.Permission;

public class PermissionsService implements PermissionsManager{

    private final Session              session;
    private final CassandraTableManager tableManager;
    
    public PermissionsService(){
    	//constructor
    }
	/**
	 * @Inject
	 * private UUID activeUUID;
	 * 
	 * or use Users.getCurrentUserKey, something like that to retrieve current user; this shouldn't need to be passed in though
	 */

	@Override
	public void addPermissionsForPropertyTypes(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		if ( checkUserHasPermissionsOnPropertyType(fqn, Permission.OWNER) ){
			int currentPermission = session.execute( tableManager.getPermissionsForPropertyType( userId, fqn ) );
			
			int permissionsToAdd = Permission.asNumber( permissions );
			//add Permission corresponds to bitwise or as numbers
		    session.execute( tableManager.setPermissionsForPropertyType( userId, fqn, currentPermission | permissionsToAdd ) );
		}	
	}

	@Override
	public void removePermissionsForPropertyTypes(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		if ( checkUserHasPermissionsOnPropertyType(fqn, Permission.OWNER) ){
			int currentPermission = session.execute( tableManager.getPermissionsForPropertyType( userId, fqn ) );
			
			int permissionsToAdd = Permission.asNumber( permissions );
			//remove Permission corresponds to bitwise or as numbers
		    session.execute( tableManager.setPermissionsForPropertyType( userId, fqn, currentPermission | ~permissionsToAdd ) );
		}	
	}

	@Override
	public void setPermissionsForPropertyTypes(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean checkUserHasPermissionsOnPropertyType(FullQualifiedName fqn, Permission permission) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void addPermissionsForEntityTypes(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removePermissionsForEntityTypes(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setPermissionsForEntityTypes(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean checkUserHasPermissionsOnEntityType(FullQualifiedName fqn, Permission permission) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void addPermissionsForEntitySets(UUID userId, FullQualifiedName type, String name,
			Set<Permission> permissions) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removePermissionsForEntitySets(UUID userId, FullQualifiedName type, String name,
			Set<Permission> permissions) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setPermissionsForEntitySets(UUID userId, FullQualifiedName type, String name,
			Set<Permission> permissions) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean checkUserHasPermissionsOnEntitySet(FullQualifiedName type, String name, Permission permission) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void addPermissionsForSchemas(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removePermissionsForSchemas(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setPermissionsForSchemas(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean checkUserHasPermissionsOnSchema(FullQualifiedName fqn, Permission permission) {
		// TODO Auto-generated method stub
		return false;
	}


}
