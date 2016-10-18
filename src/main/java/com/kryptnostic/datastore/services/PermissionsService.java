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
	//TODO currentId = current user's Id, should be obtained from Auth0 or wherever.
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
	public void addPermissionsForPropertyType(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		if ( checkUserHasPermissionsOnPropertyType(fqn, Permission.OWNER) ){
			int currentPermission = tableManager.getPermissionsForPropertyType( userId, fqn );
			
			int permissionsToAdd = Permission.asNumber( permissions );
			//add Permission corresponds to bitwise or current permission, and permissionsToAdd 
		    tableManager.setPermissionsForPropertyType( userId, fqn, currentPermission | permissionsToAdd );
		}	
	}

	@Override
	public void removePermissionsForPropertyType(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		if ( checkUserHasPermissionsOnPropertyType(fqn, Permission.OWNER) ){
			int currentPermission = tableManager.getPermissionsForPropertyType( userId, fqn );
			
			int permissionsToRemove = Permission.asNumber( permissions );
			//remove Permission corresponds to bitwise or current permission, and NOT permissionsToRemove
		    tableManager.setPermissionsForPropertyType( userId, fqn, currentPermission | ~permissionsToRemove );
		}	
	}

	@Override
	public void setPermissionsForPropertyType(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		if ( checkUserHasPermissionsOnPropertyType(fqn, Permission.OWNER) ){
			int permissionsToSet = Permission.asNumber( permissions );
		    tableManager.setPermissionsForPropertyType( userId, fqn, permissionsToSet );
		}			
	}

	@Override
	public boolean checkUserHasPermissionsOnPropertyType(FullQualifiedName fqn, Permission permission) {
		int position = Permission.getPosition( permission );

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
	public void addPermissionsForEntityType(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		if ( checkUserHasPermissionsOnEntityType(fqn, Permission.OWNER) ){
			int currentPermission = tableManager.getPermissionsForEntityType( userId, fqn );
			
			int permissionsToAdd = Permission.asNumber( permissions );
			//add Permission corresponds to bitwise or current permission, and permissionsToAdd 
		    tableManager.setPermissionsForEntityType( userId, fqn, currentPermission | permissionsToAdd );
		}			
	}

	@Override
	public void removePermissionsForEntityType(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		if ( checkUserHasPermissionsOnEntityType(fqn, Permission.OWNER) ){
			int currentPermission = tableManager.getPermissionsForEntityType( userId, fqn );
			
			int permissionsToRemove = Permission.asNumber( permissions );
			//remove Permission corresponds to bitwise or current permission, and NOT permissionsToRemove
		    tableManager.setPermissionsForEntityType( userId, fqn, currentPermission | ~permissionsToRemove );
		}	
	}

	@Override
	public void setPermissionsForEntityType(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		if ( checkUserHasPermissionsOnEntityType(fqn, Permission.OWNER) ){
			int permissionsToSet = Permission.asNumber( permissions );
		    tableManager.setPermissionsForEntityType( userId, fqn, permissionsToSet );
		}		
	}

	@Override
	public boolean checkUserHasPermissionsOnEntityType(FullQualifiedName fqn, Permission permission) {
		int position = Permission.getPosition( permission );

		boolean userHasPermission = BooleanUtils.toBoolean( 
				//check if the corresponding bit of permission is set
				    ( tableManager.getPermissionsForEntityType( currentId, fqn)  >> position ) & 1 
				);
		
		if( userHasPermission ){
			return true;
		}else{
			throw new UnauthorizedException();
		}
	}

	@Override
	public void addPermissionsForEntitySet(UUID userId, FullQualifiedName type, String name,
			Set<Permission> permissions) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removePermissionsForEntitySet(UUID userId, FullQualifiedName type, String name,
			Set<Permission> permissions) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setPermissionsForEntitySet(UUID userId, FullQualifiedName type, String name,
			Set<Permission> permissions) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean checkUserHasPermissionsOnEntitySet(FullQualifiedName type, String name, Permission permission) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void addPermissionsForSchema(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removePermissionsForSchema(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setPermissionsForSchema(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean checkUserHasPermissionsOnSchema(FullQualifiedName fqn, Permission permission) {
		// TODO Auto-generated method stub
		return false;
	}
	
	@Override
	public void addPermissionsForPropertyTypeInEntityType(UUID userId, FullQualifiedName entityTypeFqn,
			FullQualifiedName propertyTypeFqn, Set<Permission> permissions) {
		if ( checkUserHasPermissionsOnPropertyTypeInEntityType( propertyTypeFqn, entityTypeFqn, Permission.OWNER) ){
            int currentPermission = tableManager.getPermissionsForPropertyTypeInEntityType( userId, propertyTypeFqn, entityTypeFqn );
			
			int permissionsToAdd = Permission.asNumber( permissions );
			//add Permission corresponds to bitwise or current permission, and permissionsToAdd 
		    tableManager.setPermissionsForPropertyTypeInEntityType( userId, propertyTypeFqn, entityTypeFqn, currentPermission | permissionsToAdd );		
		}
	}
	
	@Override
	public void removePermissionsForPropertyTypeInEntityType(UUID userId, FullQualifiedName entityTypeFqn,
			FullQualifiedName propertyTypeFqn, Set<Permission> permissions) {
		if ( checkUserHasPermissionsOnPropertyTypeInEntityType( propertyTypeFqn, entityTypeFqn, Permission.OWNER) ){
            int currentPermission = tableManager.getPermissionsForPropertyTypeInEntityType( userId, propertyTypeFqn, entityTypeFqn );
			
			int permissionsToRemove = Permission.asNumber( permissions );
			//remove Permission corresponds to bitwise or current permission, and NOT permissionsToRemove
		    tableManager.setPermissionsForPropertyTypeInEntityType( userId, propertyTypeFqn, entityTypeFqn, currentPermission | ~permissionsToRemove );		
		}		
	}
	
	@Override
	public void setPermissionsForPropertyTypeInEntityType(UUID userId, FullQualifiedName entityTypeFqn,
			FullQualifiedName propertyTypeFqn, Set<Permission> permissions) {
		if ( checkUserHasPermissionsOnPropertyTypeInEntityType( propertyTypeFqn, entityTypeFqn, Permission.OWNER) ){
			int permissionsToSet = Permission.asNumber( permissions );
		    tableManager.setPermissionsForPropertyTypeInEntityType( userId, propertyTypeFqn, entityTypeFqn, permissionsToSet );		
		}	
	}
	
	@Override
	public boolean checkUserHasPermissionsOnPropertyTypeInEntityType(FullQualifiedName entityTypeFqn,
			FullQualifiedName propertyTypeFqn, Permission permission) {
		int position = Permission.getPosition( permission );

		boolean userHasPermission = BooleanUtils.toBoolean( 
				//check if the corresponding bit of permission is set
				    ( tableManager.getPermissionsForPropertyTypeInEntityType( currentId, propertyTypeFqn, entityTypeFqn)  >> position ) & 1 
				);
		
		if( userHasPermission ){
			return true;
		}else{
			throw new UnauthorizedException();
		}
	}

}
