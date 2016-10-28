package com.kryptnostic.datastore.services;

import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.datastore.Constants;
import com.kryptnostic.datastore.Permission;

public class PermissionsService implements PermissionsManager{

	private final Session              session;
    private final Mapper<EntityType>   entityTypeMapper;
    private final CassandraTableManager tableManager;
    private final CassandraEdmStore edmStore;
    
    public PermissionsService( Session session, MappingManager mappingManager, CassandraTableManager tableManager ){
    	this.session = session;
    	this.tableManager = tableManager;
        this.entityTypeMapper = mappingManager.mapper( EntityType.class );
        this.edmStore = mappingManager.createAccessor( CassandraEdmStore.class );
    }
	@Override
	public void addPermissionsForEntityType(String role, FullQualifiedName fqn, Set<Permission> permissions) {
		int currentPermission = getPermissionsForEntityType( role, fqn );
		
		int permissionsToAdd = Permission.asNumber( permissions );
		//add Permission corresponds to bitwise or current permission, and permissionsToAdd 
	    setPermissionsForEntityType( role, fqn, currentPermission | permissionsToAdd );
	}

	@Override
	public void removePermissionsForEntityType(String role, FullQualifiedName fqn, Set<Permission> permissions) {
		int currentPermission = getPermissionsForEntityType( role, fqn );
		
		int permissionsToRemove = Permission.asNumber( permissions );
		//remove Permission corresponds to bitwise and current permission and NOT permissionsToRemove
	    setPermissionsForEntityType( role, fqn, currentPermission & ~permissionsToRemove );
	}

	@Override
	public void setPermissionsForEntityType(String role, FullQualifiedName fqn, Set<Permission> permissions) {		
		int permissionsToSet = Permission.asNumber( permissions );
	    setPermissionsForEntityType( role, fqn, permissionsToSet );
	}

	private void setPermissionsForEntityType(String role, FullQualifiedName fqn, int permission){
		tableManager.setPermissionsForEntityType( role, fqn, permission);
	}
	
	@Override
	public boolean checkUserHasPermissionsOnEntityType( Set<String> roles, FullQualifiedName fqn, Permission permission) {
        if( roles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            int userPermission = getPermissionsForEntityType( roles, fqn );
    
            boolean userHasPermission = Permission.canDoAction( userPermission, permission );
            
            return userHasPermission;
        }
	}

    private int getPermissionsForEntityType( String role, FullQualifiedName fqn ){
        return tableManager.getPermissionsForEntityType( role, fqn );
    }
    
    private int getPermissionsForEntityType( Set<String> roles, FullQualifiedName fqn ){
        return roles.stream()
                .mapToInt( role -> getPermissionsForEntityType( role, fqn ) )
                .reduce( 0, ( a,b ) -> a | b );
    }
    
	@Override
	public void addPermissionsForEntitySet(String role, FullQualifiedName type, String name,
			Set<Permission> permissions) {
        int currentPermission = getPermissionsForEntitySet( role, type, name );
        
        int permissionsToAdd = Permission.asNumber( permissions );
        //add Permission corresponds to bitwise or current permission, and permissionsToAdd 
        setPermissionsForEntitySet( role, type, name, currentPermission | permissionsToAdd );
	}

	@Override
	public void removePermissionsForEntitySet(String role, FullQualifiedName type, String name,
			Set<Permission> permissions) {
        int currentPermission = getPermissionsForEntitySet( role, type, name );
        
        int permissionsToRemove = Permission.asNumber( permissions );
        //remove Permission corresponds to bitwise and current permission and NOT permissionsToRemove
        setPermissionsForEntitySet( role, type, name, currentPermission & ~permissionsToRemove );		
	}

	@Override
	public void setPermissionsForEntitySet(String role, FullQualifiedName type, String name,
			Set<Permission> permissions) {        
        int permissionsToSet = Permission.asNumber( permissions );
        setPermissionsForEntitySet( role, type, name, permissionsToSet );
	}

	private void setPermissionsForEntitySet(String role, FullQualifiedName type, String name, int permission){
        tableManager.setPermissionsForEntitySet( role, type, name, permission);
	}
    
	@Override
	public boolean checkUserHasPermissionsOnEntitySet( Set<String> roles, FullQualifiedName type, String name, Permission permission) {
        if( roles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            int userPermission = getPermissionsForEntitySet( roles, type, name);
    
            boolean userHasPermission = Permission.canDoAction( userPermission, permission );
            
            return userHasPermission;
        }
	}

    private int getPermissionsForEntitySet( String role, FullQualifiedName type, String name ){
        return tableManager.getPermissionsForEntitySet( role, type, name );
    }
    
    private int getPermissionsForEntitySet( Set<String> roles, FullQualifiedName type, String name ){
        return roles.stream()
                .mapToInt( role -> getPermissionsForEntitySet( role, type, name ) )
                .reduce( 0, ( a,b ) -> a | b );
    }
    
	@Override
	public void addPermissionsForPropertyTypeInEntityType(String role, FullQualifiedName entityTypeFqn,
			FullQualifiedName propertyTypeFqn, Set<Permission> permissions) {
        int currentPermission = getPermissionsForPropertyTypeInEntityType( role, entityTypeFqn, propertyTypeFqn );
		
		int permissionsToAdd = Permission.asNumber( permissions );
		//add Permission corresponds to bitwise or current permission, and permissionsToAdd 
	    setPermissionsForPropertyTypeInEntityType( role, entityTypeFqn, propertyTypeFqn, currentPermission | permissionsToAdd );
	}
		
	@Override
	public void removePermissionsForPropertyTypeInEntityType(String role, FullQualifiedName entityTypeFqn,
			FullQualifiedName propertyTypeFqn, Set<Permission> permissions) {
        int currentPermission = getPermissionsForPropertyTypeInEntityType( role, entityTypeFqn, propertyTypeFqn );
		
		int permissionsToRemove = Permission.asNumber( permissions );
		//remove Permission corresponds to bitwise and current permission with NOT permissionsToRemove
	    setPermissionsForPropertyTypeInEntityType( role, entityTypeFqn, propertyTypeFqn, currentPermission & ~permissionsToRemove );	
	}
	
	@Override
	public void setPermissionsForPropertyTypeInEntityType(String role, FullQualifiedName entityTypeFqn,
			FullQualifiedName propertyTypeFqn, Set<Permission> permissions) {
			int permissionsToSet = Permission.asNumber( permissions );
		    setPermissionsForPropertyTypeInEntityType( role, entityTypeFqn, propertyTypeFqn, permissionsToSet );	
	}
	
	private void setPermissionsForPropertyTypeInEntityType(String role, FullQualifiedName entityTypeFqn,
			FullQualifiedName propertyTypeFqn, int permission) {
		tableManager.setPermissionsForPropertyTypeInEntityType( role, entityTypeFqn, propertyTypeFqn, permission );		
	}
	
	@Override
	public boolean checkUserHasPermissionsOnPropertyTypeInEntityType( Set<String> roles, FullQualifiedName entityTypeFqn,
			FullQualifiedName propertyTypeFqn, Permission permission) {
        if( roles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {	    
            int userPermission = getPermissionsForPropertyTypeInEntityType( roles, entityTypeFqn, propertyTypeFqn );
    
            boolean userHasPermission = Permission.canDoAction( userPermission, permission );	
    		
            return userHasPermission;
        }
	}

    private int getPermissionsForPropertyTypeInEntityType( String role, FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn ){
        return tableManager.getPermissionsForPropertyTypeInEntityType( role, entityTypeFqn, propertyTypeFqn );
    }
    
    private int getPermissionsForPropertyTypeInEntityType( Set<String> roles, FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn ){
        return roles.stream()
                .mapToInt( role -> getPermissionsForPropertyTypeInEntityType( role, entityTypeFqn, propertyTypeFqn ) )
                .reduce( 0, ( a,b ) -> a | b ); 
    }
    
    @Override
    public void addPermissionsForPropertyTypeInEntitySet(String role, FullQualifiedName entityTypeFqn, String entitySetName,
            FullQualifiedName propertyTypeFqn, Set<Permission> permissions) {
        int currentPermission = getPermissionsForPropertyTypeInEntitySet( role, entityTypeFqn, entitySetName, propertyTypeFqn );
        
        int permissionsToAdd = Permission.asNumber( permissions );
        //add Permission corresponds to bitwise or current permission, and permissionsToAdd 
        setPermissionsForPropertyTypeInEntitySet( role, entityTypeFqn, entitySetName, propertyTypeFqn, currentPermission | permissionsToAdd );
    }
        
    @Override
    public void removePermissionsForPropertyTypeInEntitySet(String role, FullQualifiedName entityTypeFqn, String entitySetName,
            FullQualifiedName propertyTypeFqn, Set<Permission> permissions) {
        int currentPermission = getPermissionsForPropertyTypeInEntitySet( role, entityTypeFqn, entitySetName, propertyTypeFqn );
        
        int permissionsToRemove = Permission.asNumber( permissions );
        //remove Permission corresponds to bitwise and current permission with NOT permissionsToRemove
        setPermissionsForPropertyTypeInEntitySet( role, entityTypeFqn, entitySetName, propertyTypeFqn, currentPermission & ~permissionsToRemove );    
    }
    
    @Override
    public void setPermissionsForPropertyTypeInEntitySet(String role, FullQualifiedName entityTypeFqn, String entitySetName,
            FullQualifiedName propertyTypeFqn, Set<Permission> permissions) {
            int permissionsToSet = Permission.asNumber( permissions );
            setPermissionsForPropertyTypeInEntitySet( role, entityTypeFqn, entitySetName, propertyTypeFqn, permissionsToSet );    
    }
    
    private void setPermissionsForPropertyTypeInEntitySet(String role, FullQualifiedName entityTypeFqn, String entitySetName,
            FullQualifiedName propertyTypeFqn, int permission) {
        tableManager.setPermissionsForPropertyTypeInEntitySet( role, entityTypeFqn, entitySetName, propertyTypeFqn, permission );     
    }
    
    @Override
    public boolean checkUserHasPermissionsOnPropertyTypeInEntitySet( Set<String> roles, FullQualifiedName entityTypeFqn, String entitySetName,
            FullQualifiedName propertyTypeFqn, Permission permission) {
        if( roles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {        
            int userPermission = getPermissionsForPropertyTypeInEntitySet( roles, entityTypeFqn, entitySetName, propertyTypeFqn );
  
            boolean userHasPermission = Permission.canDoAction( userPermission, permission );   
            
            return userHasPermission;
        }
    }
    
    private int getPermissionsForPropertyTypeInEntitySet( String role, FullQualifiedName entityTypeFqn, String entitySetName, FullQualifiedName propertyTypeFqn ){
        return tableManager.getPermissionsForPropertyTypeInEntitySet( role, entityTypeFqn, entitySetName, propertyTypeFqn );
    }
    
    private int getPermissionsForPropertyTypeInEntitySet( Set<String> roles, FullQualifiedName entityTypeFqn, String entitySetName, FullQualifiedName propertyTypeFqn ){
        return roles.stream()
                .mapToInt( role -> getPermissionsForPropertyTypeInEntitySet( role, entityTypeFqn, entitySetName, propertyTypeFqn ) )
                .reduce( 0, ( a,b ) -> a | b ); 
    }
    
	@Override
	public void removePermissionsForEntityType( FullQualifiedName fqn ){
        tableManager.deleteFromEntityTypesAclsTable( fqn );
	}
	
    @Override
    public void removePermissionsForEntitySet( FullQualifiedName entityTypeName, String entitySetName ){
        tableManager.deleteFromEntitySetsAclsTable( entityTypeName, entitySetName );
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
    public void removePermissionsForPropertyTypeInEntitySet( FullQualifiedName entityTypeFqn, String entitySetName,
            FullQualifiedName propertyTypeFqn) {
        tableManager.deleteFromPropertyTypeInEntitySetsAclsTable( entityTypeFqn, entitySetName, propertyTypeFqn );
    }
    
    @Override
    public void removePermissionsForPropertyTypeInEntitySet( FullQualifiedName entityTypeFqn, String entitySetName ) {
        tableManager.deleteFromPropertyTypeInEntitySetsAclsTable( entityTypeFqn, entitySetName );
    }
        
}
