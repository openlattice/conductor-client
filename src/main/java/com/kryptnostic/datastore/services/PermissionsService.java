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

	private final Session              session;
    private final Mapper<EntityType>   entityTypeMapper;
    private final CassandraTableManager tableManager;
    
    public PermissionsService( Session session, MappingManager mappingManager, CassandraTableManager tableManager ){
    	this.session = session;
    	this.tableManager = tableManager;
        this.entityTypeMapper = mappingManager.mapper( EntityType.class );
    }


	@Override
	public void addPermissionsForPropertyType(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		int currentPermission = tableManager.getPermissionsForPropertyType( userId, fqn );
			
		int permissionsToAdd = Permission.asNumber( permissions );
		//add Permission corresponds to bitwise or current permission, and permissionsToAdd 
		setPermissionsForPropertyType( userId, fqn, currentPermission | permissionsToAdd );	
	}
	
	@Override
	public void removePermissionsForPropertyType(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		int currentPermission = tableManager.getPermissionsForPropertyType( userId, fqn );
		
		int permissionsToRemove = Permission.asNumber( permissions );
		//remove Permission corresponds to bitwise or current permission, and NOT permissionsToRemove
	    setPermissionsForPropertyType( userId, fqn, currentPermission | ~permissionsToRemove );
	}

	@Override
	public void setPermissionsForPropertyType(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		int permissionsToSet = Permission.asNumber( permissions );
	    setPermissionsForPropertyType( userId, fqn, permissionsToSet );
	}

	private void setPermissionsForPropertyType( UUID userId, FullQualifiedName fqn, int permission ){
		    tableManager.setPermissionsForPropertyType( userId, fqn, permission );
	}
	
	@Override
	public boolean checkUserHasPermissionsOnPropertyType( UUID userId, FullQualifiedName fqn, Permission permission) {
		boolean userHasPermission = Permission.canDoAction( 
				tableManager.getPermissionsForPropertyType( userId, fqn), 
				permission );
		
        return userHasPermission;
	}

	@Override
	public void addPermissionsForEntityType(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		int currentPermission = tableManager.getPermissionsForEntityType( userId, fqn );
		
		int permissionsToAdd = Permission.asNumber( permissions );
		//add Permission corresponds to bitwise or current permission, and permissionsToAdd 
	    setPermissionsForEntityType( userId, fqn, currentPermission, currentPermission | permissionsToAdd );
	}

	@Override
	public void removePermissionsForEntityType(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		int currentPermission = tableManager.getPermissionsForEntityType( userId, fqn );
		
		int permissionsToRemove = Permission.asNumber( permissions );
		//remove Permission corresponds to bitwise or current permission, and NOT permissionsToRemove
	    setPermissionsForEntityType( userId, fqn, currentPermission, currentPermission | ~permissionsToRemove );
	}

	@Override
	public void setPermissionsForEntityType(UUID userId, FullQualifiedName fqn, Set<Permission> permissions) {
		int currentPermission = tableManager.getPermissionsForEntityType( userId, fqn );
		
		int permissionsToSet = Permission.asNumber( permissions );
	    setPermissionsForEntityType( userId, fqn, currentPermission, permissionsToSet );
	}

	private void setPermissionsForEntityType(UUID userId, FullQualifiedName fqn, int oldPermission, int newPermission){
		tableManager.setPermissionsForEntityType( userId, fqn, newPermission);

		boolean oldPermissionCanAlter = Permission.canDoAction( oldPermission, Permission.ALTER );
		boolean newPermissionCanAlter = Permission.canDoAction( newPermission, Permission.ALTER );
		
		if( !oldPermissionCanAlter && newPermissionCanAlter ){
			addToEntityTypesAlterRightsTable( userId, fqn );
			// For now, getting ALTER/OWNER permission for an entity type would automatically allow user to discover all discoverable properties in the entity type
		    updateDiscoverablePropertyTypesForEntityType( userId, fqn );
		} else if( oldPermissionCanAlter && !newPermissionCanAlter ){
			removeFromEntityTypesAlterRightsTable( userId, fqn );
		}
	}
		
	private void updateDiscoverablePropertyTypesForEntityType( UUID userId, FullQualifiedName entityTypeFqn ){
		entityTypeMapper.get( entityTypeFqn.getNamespace(), entityTypeFqn.getName() )
		    .getProperties()
		    .stream()
		    .filter( propertyTypeFqn -> checkUserHasPermissionsOnPropertyType( userId, propertyTypeFqn, Permission.DISCOVER ) )
		    .forEach( propertyTypeFqn -> {
		    	addPermissionsForPropertyTypeInEntityType( userId, entityTypeFqn, propertyTypeFqn, ImmutableSet.of(Permission.DISCOVER) );
		    });
	}
	
	@Override
	public boolean checkUserHasPermissionsOnEntityType( UUID userId, FullQualifiedName fqn, Permission permission) {
		boolean userHasPermission = Permission.canDoAction( 
				tableManager.getPermissionsForEntityType( userId, fqn), 
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
	public boolean checkUserHasPermissionsOnEntitySet( UUID userId, FullQualifiedName type, String name, Permission permission) {
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
	public boolean checkUserHasPermissionsOnSchema( UUID userId, FullQualifiedName fqn, Permission permission) {
		// TODO Auto-generated method stub
		return false;
	}
	
	@Override
	public void addPermissionsForPropertyTypeInEntityType(UUID userId, FullQualifiedName entityTypeFqn,
			FullQualifiedName propertyTypeFqn, Set<Permission> permissions) {
        int currentPermission = tableManager.getPermissionsForPropertyTypeInEntityType( userId, propertyTypeFqn, entityTypeFqn );
		
		int permissionsToAdd = Permission.asNumber( permissions );
		//add Permission corresponds to bitwise or current permission, and permissionsToAdd 
	    setPermissionsForPropertyTypeInEntityType( userId, entityTypeFqn, propertyTypeFqn, currentPermission | permissionsToAdd );
		// If user has DISCOVER/READ rights relatively, should have DISCOVER/READ right absolutely for the property type
		if( Permission.canDoAction( permissionsToAdd, Permission.READ) ){
		    addPermissionsForPropertyType( userId, propertyTypeFqn, ImmutableSet.of(Permission.READ) );
		} else if ( Permission.canDoAction( permissionsToAdd, Permission.DISCOVER ) ){
		    addPermissionsForPropertyType( userId, propertyTypeFqn, ImmutableSet.of(Permission.DISCOVER) );			
		}
	}
		
	@Override
	public void removePermissionsForPropertyTypeInEntityType(UUID userId, FullQualifiedName entityTypeFqn,
			FullQualifiedName propertyTypeFqn, Set<Permission> permissions) {
        int currentPermission = tableManager.getPermissionsForPropertyTypeInEntityType( userId, entityTypeFqn, propertyTypeFqn );
		
		int permissionsToRemove = Permission.asNumber( permissions );
		//remove Permission corresponds to bitwise or current permission, and NOT permissionsToRemove
	    setPermissionsForPropertyTypeInEntityType( userId, entityTypeFqn, propertyTypeFqn, currentPermission | ~permissionsToRemove );		
	}
	
	@Override
	public void setPermissionsForPropertyTypeInEntityType(UUID userId, FullQualifiedName entityTypeFqn,
			FullQualifiedName propertyTypeFqn, Set<Permission> permissions) {
			int permissionsToSet = Permission.asNumber( permissions );
		    setPermissionsForPropertyTypeInEntityType( userId, entityTypeFqn, propertyTypeFqn, permissionsToSet );		
	}
	
	private void setPermissionsForPropertyTypeInEntityType(UUID userId, FullQualifiedName entityTypeFqn,
			FullQualifiedName propertyTypeFqn, int permission) {
		tableManager.setPermissionsForPropertyTypeInEntityType( userId, entityTypeFqn, propertyTypeFqn, permission );		
	}
	
	@Override
	public boolean checkUserHasPermissionsOnPropertyTypeInEntityType( UUID userId, FullQualifiedName entityTypeFqn,
			FullQualifiedName propertyTypeFqn, Permission permission) {
		boolean userHasPermission = Permission.canDoAction( 
				tableManager.getPermissionsForPropertyTypeInEntityType( userId, entityTypeFqn, propertyTypeFqn), 
				permission );	
		
        return userHasPermission;
	}
	
	@Override
	public void addToEntityTypesAlterRightsTable( UUID userId, FullQualifiedName entityTypeFqn ){
		tableManager.addToEntityTypesAlterRightsTable( userId, entityTypeFqn );
	}
	
	@Override
	public void removeFromEntityTypesAlterRightsTable( UUID userId, FullQualifiedName entityTypeFqn ){
		tableManager.removeFromEntityTypesAlterRightsTable( userId, entityTypeFqn );
	}
    
	@Override
	public void removePermissionsForPropertyType( FullQualifiedName fqn ){
        tableManager.deleteFromPropertyTypesAclsTable( fqn );
	}
	
	@Override
	public void removePermissionsForEntityType( FullQualifiedName fqn ){
        tableManager.deleteFromEntityTypesAclsTable( fqn );
	}
	
	@Override
	public void removePermissionsForPropertyTypeInEntityType( FullQualifiedName entityTypeFqn,
			FullQualifiedName propertyTypeFqn) {
		tableManager.deleteFromPropertyTypeInEntityTypesAclsTable( entityTypeFqn, propertyTypeFqn );
	}
	
	@Override
	public void removePermissionsForPropertyTypeInEntityType( FullQualifiedName entityTypeFqn ){
        tableManager.deleteFromPropertyTypeInEntityTypesAclsTable( entityTypeFqn );
	}
	@Override
	public void removeFromEntityTypesAlterRightsTable( FullQualifiedName entityTypeFqn ){
        tableManager.removeFromEntityTypesAlterRightsTable( entityTypeFqn );
	}
}
