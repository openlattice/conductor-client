package com.kryptnostic.datastore.services;

import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.datastax.driver.core.Session;
import com.kryptnostic.datastore.Permission;
import com.kryptnostic.instrumentation.v1.exceptions.types.UnauthorizedException;

public class PermissionsService implements PermissionsManager{
	/** 
	 * Being of debug
	 */
	private UUID                   currentId;
	public void setCurrentUserIdForDebug( UUID currentId ){
		this.currentId = currentId;
	}
	/**
	 * End of debug
	 */

    private final Session              session;
    private final CassandraTableManager tableManager;
    
    public PermissionsService( Session session, CassandraTableManager tableManager ){
    	this.session = session;
    	this.tableManager = tableManager;
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
			int currentPermission = tableManager.getPermissionsForPropertyType( userId, fqn );
			
			int permissionsToAdd = Permission.asNumber( permissions );
			//add Permission corresponds to bitwise or current permission, and permissionsToAdd 
		    tableManager.setPermissionsForPropertyType( userId, fqn, currentPermission | permissionsToAdd );
		}	
	}

	@Override
	public void removePermissionsForPropertyTypes(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		if ( checkUserHasPermissionsOnPropertyType(fqn, Permission.OWNER) ){
			int currentPermission = tableManager.getPermissionsForPropertyType( userId, fqn );
			
			int permissionsToRemove = Permission.asNumber( permissions );
			//remove Permission corresponds to bitwise or current permission, and NOT permissionsToRemove
		    tableManager.setPermissionsForPropertyType( userId, fqn, currentPermission | ~permissionsToRemove );
		}	
	}

	@Override
	public void setPermissionsForPropertyTypes(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		if ( checkUserHasPermissionsOnPropertyType(fqn, Permission.OWNER) ){
			int permissionsToSet = Permission.asNumber( permissions );
		    tableManager.setPermissionsForPropertyType( userId, fqn, permissionsToSet );
		}			
	}

	@Override
	public boolean checkUserHasPermissionsOnPropertyType(FullQualifiedName fqn, Permission permission) {
		//TODO currentId = current user's Id, should be obtained from Auth0 or wherever.
		int position = Permission.hasPosition( permission );

		boolean userHasPermission = BooleanUtils.toBoolean( 
				//check if the corresponding bit of permission is set
				    ( tableManager.getPermissionsForPropertyType( currentId, fqn)  >> position ) & 1 
				);
		if( userHasPermission ){
			return true;
		}else{
			throw new UnauthorizedException();
		}
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
