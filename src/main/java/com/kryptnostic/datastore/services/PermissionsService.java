package com.kryptnostic.datastore.services;

import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.conductor.rpc.odata.EntityType;
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
    private final Mapper<EntityType>   entityTypeMapper;
    private final CassandraTableManager tableManager;
    
    public PermissionsService( Session session, MappingManager mappingManager, CassandraTableManager tableManager ){
    	this.session = session;
    	this.tableManager = tableManager;
        this.entityTypeMapper = mappingManager.mapper( EntityType.class );
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
               addPermissionsForPropertyType( userId, fqn, permissions, true );
		}	
	}
	
	private void addPermissionsForPropertyType(UUID userId, FullQualifiedName fqn, Set<Permission> permissions, boolean userHasPermission) {
		int currentPermission = tableManager.getPermissionsForPropertyType( userId, fqn );
			
		int permissionsToAdd = Permission.asNumber( permissions );
	    //add Permission corresponds to bitwise or current permission, and permissionsToAdd 
		setPermissionsForPropertyType( userId, fqn, currentPermission | permissionsToAdd );
	}

	@Override
	public void removePermissionsForPropertyType(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		if ( checkUserHasPermissionsOnPropertyType(fqn, Permission.OWNER) ){
			int currentPermission = tableManager.getPermissionsForPropertyType( userId, fqn );
			
			int permissionsToRemove = Permission.asNumber( permissions );
			//remove Permission corresponds to bitwise or current permission, and NOT permissionsToRemove
		    setPermissionsForPropertyType( userId, fqn, currentPermission | ~permissionsToRemove );
		}	
	}

	@Override
	public void setPermissionsForPropertyType(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		if ( checkUserHasPermissionsOnPropertyType(fqn, Permission.OWNER) ){
			int permissionsToSet = Permission.asNumber( permissions );
		    setPermissionsForPropertyType( userId, fqn, permissionsToSet );
		}			
	}

	private void setPermissionsForPropertyType( UUID userId, FullQualifiedName fqn, int permission ){
		    tableManager.setPermissionsForPropertyType( userId, fqn, permission );
	}
	
	@Override
	public boolean checkUserHasPermissionsOnPropertyType(FullQualifiedName fqn, Permission permission) {
		boolean userHasPermission = Permission.canDoAction( 
				tableManager.getPermissionsForPropertyType( currentId, fqn), 
				permission );
		
        return userHasPermission;
	}

	@Override
	public void addPermissionsForEntityType(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		if ( checkUserHasPermissionsOnEntityType(fqn, Permission.OWNER) ){
			int currentPermission = tableManager.getPermissionsForEntityType( userId, fqn );
			
			int permissionsToAdd = Permission.asNumber( permissions );
			//add Permission corresponds to bitwise or current permission, and permissionsToAdd 
		    setPermissionsForEntityType( userId, fqn, currentPermission, currentPermission | permissionsToAdd );
		}			
	}

	@Override
	public void removePermissionsForEntityType(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		if ( checkUserHasPermissionsOnEntityType(fqn, Permission.OWNER) ){
			int currentPermission = tableManager.getPermissionsForEntityType( userId, fqn );
			
			int permissionsToRemove = Permission.asNumber( permissions );
			//remove Permission corresponds to bitwise or current permission, and NOT permissionsToRemove
		    setPermissionsForEntityType( userId, fqn, currentPermission, currentPermission | ~permissionsToRemove );
		}	
	}

	@Override
	public void setPermissionsForEntityType(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		if ( checkUserHasPermissionsOnEntityType(fqn, Permission.OWNER) ){
			int currentPermission = tableManager.getPermissionsForEntityType( userId, fqn );
			
			int permissionsToSet = Permission.asNumber( permissions );
		    setPermissionsForEntityType( userId, fqn, currentPermission, permissionsToSet );
		}		
	}

	private void setPermissionsForEntityType(UUID userId, FullQualifiedName fqn, int oldPermission, int newPermission){
		tableManager.setPermissionsForEntityType( userId, fqn, newPermission);

		boolean oldPermissionCanAlter = Permission.canDoAction( oldPermission, Permission.ALTER );
		boolean newPermissionCanAlter = Permission.canDoAction( newPermission, Permission.ALTER );
		
		if( !oldPermissionCanAlter && newPermissionCanAlter ){
			addUserToEntityTypesAlterRightsTable( userId, fqn );
			// For now, getting ALTER/OWNER permission for an entity type would automatically allow user to discover all discoverable properties in the entity type
		    updateDiscoverablePropertyTypesForEntityType( userId, fqn );
		} else if( oldPermissionCanAlter && !newPermissionCanAlter ){
			removeUserFromEntityTypesAlterRightsTable( userId, fqn );
		}
	}
	
	private void addUserToEntityTypesAlterRightsTable( UUID userId, FullQualifiedName entityTypeFqn ){
		tableManager.addToEntityTypesAlterRightsTable( userId, entityTypeFqn );
	}
	
	private void removeUserFromEntityTypesAlterRightsTable( UUID userId, FullQualifiedName entityTypeFqn ){
		tableManager.removeFromEntityTypesAlterRightsTable( userId, entityTypeFqn );
	}
	
	private void updateDiscoverablePropertyTypesForEntityType( UUID userId, FullQualifiedName entityTypeFqn ){
		entityTypeMapper.get( entityTypeFqn.getNamespace(), entityTypeFqn.getName() )
		    .getProperties()
		    .stream()
		    .filter( propertyTypeFqn -> checkUserHasPermissionsOnPropertyType( propertyTypeFqn, Permission.DISCOVER ) )
		    .forEach( propertyTypeFqn -> {
		    	addPermissionsForPropertyTypeInEntityType( userId, entityTypeFqn, propertyTypeFqn, ImmutableSet.of(Permission.DISCOVER), true);
		    });
	}
	
	@Override
	public boolean checkUserHasPermissionsOnEntityType(FullQualifiedName fqn, Permission permission) {
		boolean userHasPermission = Permission.canDoAction( 
				tableManager.getPermissionsForEntityType( currentId, fqn), 
				permission );		
		
        return userHasPermission;
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
			addPermissionsForPropertyTypeInEntityType( userId, entityTypeFqn, propertyTypeFqn, permissions, true);
		}
	}
	
	private void addPermissionsForPropertyTypeInEntityType(UUID userId, FullQualifiedName entityTypeFqn,
			FullQualifiedName propertyTypeFqn, Set<Permission> permissions, boolean userHasPermission) {
        int currentPermission = tableManager.getPermissionsForPropertyTypeInEntityType( userId, propertyTypeFqn, entityTypeFqn );
		
		int permissionsToAdd = Permission.asNumber( permissions );
		//add Permission corresponds to bitwise or current permission, and permissionsToAdd 
	    setPermissionsForPropertyTypeInEntityType( userId, propertyTypeFqn, entityTypeFqn, currentPermission | permissionsToAdd );
		// If user has DISCOVER/READ rights relatively, should have DISCOVER/READ right absolutely for the property type
		if( Permission.canDoAction( permissionsToAdd, Permission.READ) ){
		    addPermissionsForPropertyType( userId, propertyTypeFqn, ImmutableSet.of(Permission.READ), true );
		} else if ( Permission.canDoAction( permissionsToAdd, Permission.DISCOVER ) ){
		    addPermissionsForPropertyType( userId, propertyTypeFqn, ImmutableSet.of(Permission.DISCOVER), true );			
		}
	}
	
	@Override
	public void removePermissionsForPropertyTypeInEntityType(UUID userId, FullQualifiedName entityTypeFqn,
			FullQualifiedName propertyTypeFqn, Set<Permission> permissions) {
		if ( checkUserHasPermissionsOnPropertyTypeInEntityType( propertyTypeFqn, entityTypeFqn, Permission.OWNER) ){
            int currentPermission = tableManager.getPermissionsForPropertyTypeInEntityType( userId, propertyTypeFqn, entityTypeFqn );
			
			int permissionsToRemove = Permission.asNumber( permissions );
			//remove Permission corresponds to bitwise or current permission, and NOT permissionsToRemove
		    setPermissionsForPropertyTypeInEntityType( userId, propertyTypeFqn, entityTypeFqn, currentPermission | ~permissionsToRemove );		
		}		
	}
	
	@Override
	public void setPermissionsForPropertyTypeInEntityType(UUID userId, FullQualifiedName entityTypeFqn,
			FullQualifiedName propertyTypeFqn, Set<Permission> permissions) {
		if ( checkUserHasPermissionsOnPropertyTypeInEntityType( propertyTypeFqn, entityTypeFqn, Permission.OWNER) ){
			int permissionsToSet = Permission.asNumber( permissions );
		    setPermissionsForPropertyTypeInEntityType( userId, propertyTypeFqn, entityTypeFqn, permissionsToSet );		
		}	
	}
	
	private void setPermissionsForPropertyTypeInEntityType(UUID userId, FullQualifiedName entityTypeFqn,
			FullQualifiedName propertyTypeFqn, int permission) {
		tableManager.setPermissionsForPropertyTypeInEntityType( userId, propertyTypeFqn, entityTypeFqn, permission );		
	}
	
	@Override
	public boolean checkUserHasPermissionsOnPropertyTypeInEntityType(FullQualifiedName entityTypeFqn,
			FullQualifiedName propertyTypeFqn, Permission permission) {
		boolean userHasPermission = Permission.canDoAction( 
				tableManager.getPermissionsForPropertyTypeInEntityType( currentId, propertyTypeFqn, entityTypeFqn), 
				permission );	
		
        return userHasPermission;
	}

}
