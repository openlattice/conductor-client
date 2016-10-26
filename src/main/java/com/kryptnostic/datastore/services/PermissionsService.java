package com.kryptnostic.datastore.services;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.datastore.Constants;
import com.kryptnostic.datastore.Permission;
import com.kryptnostic.instrumentation.v1.exceptions.types.UnauthorizedException;

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
	public void addPermissionsForPropertyType(String role, FullQualifiedName fqn, Set<Permission> permissions) {
		int currentPermission = getPermissionsForPropertyType( role, fqn );
			
		int permissionsToAdd = Permission.asNumber( permissions );
		//add Permission corresponds to bitwise or current permission, and permissionsToAdd 
		setPermissionsForPropertyType( role, fqn, currentPermission | permissionsToAdd );	
	}
	
	@Override
	public void removePermissionsForPropertyType(String role, FullQualifiedName fqn, Set<Permission> permissions) {
		int currentPermission = getPermissionsForPropertyType( role, fqn );
		
		int permissionsToRemove = Permission.asNumber( permissions );
		//remove Permission corresponds to bitwise and current permission with NOT permissionsToRemove
	    setPermissionsForPropertyType( role, fqn, currentPermission & ~permissionsToRemove );
	}

	@Override
	public void setPermissionsForPropertyType(String role, FullQualifiedName fqn, Set<Permission> permissions) {
		int permissionsToSet = Permission.asNumber( permissions );
	    setPermissionsForPropertyType( role, fqn, permissionsToSet );
	}

	private void setPermissionsForPropertyType( String role, FullQualifiedName fqn, int permission ){
		    tableManager.setPermissionsForPropertyType( role, fqn, permission );
	}
	
	@Override
	public boolean checkUserHasPermissionsOnPropertyType( Set<String> roles, FullQualifiedName fqn, Permission permission) {
	    if( roles.contains( Constants.ROLE_ADMIN ) ){
	        return true;
	    } else{
    	    int userPermission = getPermissionsForPropertyType( roles, fqn);
    		boolean userHasPermission = Permission.canDoAction( userPermission, permission );
    		
            return userHasPermission;
	    }
	}
	
    private int getPermissionsForPropertyType( String role, FullQualifiedName fqn ){
        return tableManager.getPermissionsForPropertyType( role, fqn );
    }
    
    private int getPermissionsForPropertyType( Set<String> roles, FullQualifiedName fqn ){
        return roles.stream()
                .mapToInt( role -> getPermissionsForPropertyType( role, fqn ) )
                .reduce( 0, ( a,b ) -> a | b );
    }

	@Override
	public void addPermissionsForEntityType(String role, FullQualifiedName fqn, Set<Permission> permissions) {
		int currentPermission = getPermissionsForEntityType( role, fqn );
		
		int permissionsToAdd = Permission.asNumber( permissions );
		//add Permission corresponds to bitwise or current permission, and permissionsToAdd 
	    setPermissionsForEntityType( role, fqn, currentPermission, currentPermission | permissionsToAdd );
	}

	@Override
	public void removePermissionsForEntityType(String role, FullQualifiedName fqn, Set<Permission> permissions) {
		int currentPermission = getPermissionsForEntityType( role, fqn );
		
		int permissionsToRemove = Permission.asNumber( permissions );
		//remove Permission corresponds to bitwise and current permission and NOT permissionsToRemove
	    setPermissionsForEntityType( role, fqn, currentPermission, currentPermission & ~permissionsToRemove );
	}

	@Override
	public void setPermissionsForEntityType(String role, FullQualifiedName fqn, Set<Permission> permissions) {
		int currentPermission = getPermissionsForEntityType( role, fqn );
		
		int permissionsToSet = Permission.asNumber( permissions );
	    setPermissionsForEntityType( role, fqn, currentPermission, permissionsToSet );
	}

	private void setPermissionsForEntityType(String role, FullQualifiedName fqn, int oldPermission, int newPermission){
		tableManager.setPermissionsForEntityType( role, fqn, newPermission);

		boolean oldPermissionCanAlter = Permission.canDoAction( oldPermission, Permission.ALTER );
		boolean newPermissionCanAlter = Permission.canDoAction( newPermission, Permission.ALTER );
		
		if( !oldPermissionCanAlter && newPermissionCanAlter ){
			addToEntityTypesAlterRightsTable( role, fqn );
			// For now, getting ALTER/OWNER permission for an entity type would automatically allow user to discover all discoverable properties in the entity type
		    updateDiscoverablePropertyTypesForEntityType( role, fqn );
		} else if( oldPermissionCanAlter && !newPermissionCanAlter ){
			removeFromEntityTypesAlterRightsTable( role, fqn );
		}
	}
	
	private void updateDiscoverablePropertyTypesForEntityType( String role, FullQualifiedName entityTypeFqn ){
		entityTypeMapper.get( entityTypeFqn.getNamespace(), entityTypeFqn.getName() )
		    .getProperties()
		    .stream()
		    .filter( propertyTypeFqn -> checkUserHasPermissionsOnPropertyType( ImmutableSet.of(role), propertyTypeFqn, Permission.DISCOVER ) )
		    .forEach( propertyTypeFqn -> {
		    	addPermissionsForPropertyTypeInEntityType( role, entityTypeFqn, propertyTypeFqn, ImmutableSet.of(Permission.DISCOVER) );
		    });
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
        setPermissionsForEntitySet( role, type, name, currentPermission, currentPermission | permissionsToAdd );
	}

	@Override
	public void removePermissionsForEntitySet(String role, FullQualifiedName type, String name,
			Set<Permission> permissions) {
        int currentPermission = getPermissionsForEntitySet( role, type, name );
        
        int permissionsToRemove = Permission.asNumber( permissions );
        //remove Permission corresponds to bitwise and current permission and NOT permissionsToRemove
        setPermissionsForEntitySet( role, type, name, currentPermission, currentPermission & ~permissionsToRemove );		
	}

	@Override
	public void setPermissionsForEntitySet(String role, FullQualifiedName type, String name,
			Set<Permission> permissions) {
        int currentPermission = getPermissionsForEntitySet( role, type, name );
        
        int permissionsToSet = Permission.asNumber( permissions );
        setPermissionsForEntitySet( role, type, name, currentPermission, permissionsToSet );
	}

	private void setPermissionsForEntitySet(String role, FullQualifiedName type, String name, int oldPermission, int newPermission){
        tableManager.setPermissionsForEntitySet( role, type, name, newPermission);

        boolean oldPermissionCanAlter = Permission.canDoAction( oldPermission, Permission.ALTER );
        boolean newPermissionCanAlter = Permission.canDoAction( newPermission, Permission.ALTER );
        
        if( !oldPermissionCanAlter && newPermissionCanAlter ){
            addToEntitySetsAlterRightsTable( role, type, name );
            // For now, getting ALTER/OWNER permission for an entity type would automatically allow user to discover all discoverable properties in the entity type
            updateDiscoverablePropertyTypesForEntitySet( role, type, name );
        } else if( oldPermissionCanAlter && !newPermissionCanAlter ){
            removeFromEntitySetsAlterRightsTable( role, type, name );
        }	    
	}
	
    private void updateDiscoverablePropertyTypesForEntitySet( String role, FullQualifiedName entityTypeFqn, String entitySetName ){
        entityTypeMapper.get( entityTypeFqn.getNamespace(), entityTypeFqn.getName() )
            .getProperties()
            .stream()
            .filter( propertyTypeFqn -> checkUserHasPermissionsOnPropertyType( ImmutableSet.of(role), propertyTypeFqn, Permission.DISCOVER ) )
            .forEach( propertyTypeFqn -> {
                addPermissionsForPropertyTypeInEntitySet( role, entityTypeFqn, entitySetName, propertyTypeFqn, ImmutableSet.of(Permission.DISCOVER) );
            });
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
	public void addPermissionsForSchema(String role, FullQualifiedName fqn, Set<Permission> permissions) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removePermissionsForSchema(String role, FullQualifiedName fqn, Set<Permission> permissions) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setPermissionsForSchema(String role, FullQualifiedName fqn, Set<Permission> permissions) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean checkUserHasPermissionsOnSchema( Set<String> roles, FullQualifiedName fqn, Permission permission) {
		// TODO Auto-generated method stub
		return false;
	}
	
	@Override
	public void addPermissionsForPropertyTypeInEntityType(String role, FullQualifiedName entityTypeFqn,
			FullQualifiedName propertyTypeFqn, Set<Permission> permissions) {
        int currentPermission = getPermissionsForPropertyTypeInEntityType( role, entityTypeFqn, propertyTypeFqn );
		
		int permissionsToAdd = Permission.asNumber( permissions );
		//add Permission corresponds to bitwise or current permission, and permissionsToAdd 
	    setPermissionsForPropertyTypeInEntityType( role, entityTypeFqn, propertyTypeFqn, currentPermission | permissionsToAdd );
		// If user has DISCOVER rights relatively, should have DISCOVER right absolutely for the property type
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
        // If user has DISCOVER rights relatively, should have DISCOVER right absolutely for the property type
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
    public void addToEntityTypesAlterRightsTable( String role, FullQualifiedName entityTypeFqn ){
        tableManager.addToEntityTypesAlterRightsTable( role, entityTypeFqn );
    }
    
    @Override
    public void removeFromEntityTypesAlterRightsTable( String role, FullQualifiedName entityTypeFqn ){
        tableManager.removeFromEntityTypesAlterRightsTable( role, entityTypeFqn );
    }
    
    @Override
    public void addToEntitySetsAlterRightsTable( String role, FullQualifiedName entityTypeFqn, String name ){
        tableManager.addToEntitySetsAlterRightsTable( role, entityTypeFqn, name );
    }
    
    @Override
    public void removeFromEntitySetsAlterRightsTable( String role, FullQualifiedName entityTypeFqn, String name ){
        tableManager.removeFromEntitySetsAlterRightsTable( role, entityTypeFqn, name );
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
    
    @Override
    public void removeFromEntityTypesAlterRightsTable( FullQualifiedName entityTypeFqn ){
        tableManager.removeFromEntityTypesAlterRightsTable( entityTypeFqn );
    }
    
    @Override
    public void removeFromEntitySetsAlterRightsTable( FullQualifiedName entityTypeFqn, String entitySetName ){
        tableManager.removeFromEntitySetsAlterRightsTable( entityTypeFqn, entitySetName );
    }
    
    @Override
    public void derivePermissions( FullQualifiedName entityTypeFqn, Set<FullQualifiedName> properties, String derivedOption){
        switch( derivedOption ){
            case "BothTypes": 
                inheritPermissionsFromBothTypes( entityTypeFqn, properties );
                break;
            case "PropertyType":
                inheritPermissionsFromBothTypes( entityTypeFqn, properties );
                break;
            default: // Do nothing
        }
    }
    
    @Override
    public void inheritPermissionsFromBothTypes( String role, FullQualifiedName entityTypeFqn, Set<FullQualifiedName> properties ){
        int entityTypePermission = getPermissionsForEntityType( role, entityTypeFqn );
        inheritPermissionsFromBothTypes( role, entityTypeFqn, entityTypePermission, properties);
    }
    
    private void inheritPermissionsFromBothTypes( String role, FullQualifiedName entityTypeFqn, int entityTypePermission, Set<FullQualifiedName> properties){
        properties.forEach( propertyTypeFqn -> {
            int propertyTypePermission = getPermissionsForPropertyType( role, propertyTypeFqn );
            boolean modifiedReadOrWrite = false;
            
            if( Permission.canDoAction( entityTypePermission, Permission.READ ) && Permission.canDoAction( propertyTypePermission, Permission.READ ) ){
                addPermissionsForPropertyTypeInEntityType( role, entityTypeFqn, propertyTypeFqn, ImmutableSet.of(Permission.READ) );
                modifiedReadOrWrite = true;
            }
            
            if( Permission.canDoAction( entityTypePermission, Permission.WRITE ) && Permission.canDoAction( propertyTypePermission, Permission.WRITE ) ){
                addPermissionsForPropertyTypeInEntityType( role, entityTypeFqn, propertyTypeFqn, ImmutableSet.of(Permission.WRITE) );
                modifiedReadOrWrite = true;
            }
            
            if( !modifiedReadOrWrite && Permission.canDoAction( entityTypePermission, Permission.DISCOVER ) && Permission.canDoAction( propertyTypePermission, Permission.DISCOVER ) ){
                addPermissionsForPropertyTypeInEntityType( role, entityTypeFqn, propertyTypeFqn, ImmutableSet.of(Permission.DISCOVER) );
            }
        });
    }
   
    @Override
    public void inheritPermissionsFromBothTypes( FullQualifiedName entityTypeFqn, Set<FullQualifiedName> properties ){
        // Get all the permission info of an entity Type. There is a list of roles involved there.
        ResultSet resultSet = tableManager.getAclsForEntityType( entityTypeFqn ); 
        Map<String, Integer> rolesToPermissions = EdmDetailsAdapter.aclAdapter( resultSet );
        for( Map.Entry<String, Integer> entry : rolesToPermissions.entrySet() ){
            inheritPermissionsFromBothTypes( entry.getKey(), entityTypeFqn, entry.getValue(), properties);
            //TODO rethink if this is a good design choice; Cassandra seems to have automatic batch statement if the partition key is the same. For this reason you may want to do outer for loop with properties instead.
        }
    }
   
    @Override
    public void inheritPermissionsFromPropertyType( String role, FullQualifiedName entityTypeFqn, Set<FullQualifiedName> properties){
        properties.forEach( propertyTypeFqn -> {
            int propertyTypePermission = getPermissionsForPropertyType( role, propertyTypeFqn );
            boolean modifiedRead = false;
            boolean modifiedWrite = false;
            
            if( Permission.canDoAction( propertyTypePermission, Permission.READ ) ){
                addPermissionsForPropertyTypeInEntityType( role, entityTypeFqn, propertyTypeFqn, ImmutableSet.of(Permission.READ) );
                if( !modifiedRead ){
                    addPermissionsForEntityType( role, entityTypeFqn, ImmutableSet.of(Permission.READ));
                    modifiedRead = true;
                }
            }
            
            if( Permission.canDoAction( propertyTypePermission, Permission.WRITE ) ){
                addPermissionsForPropertyTypeInEntityType( role, entityTypeFqn, propertyTypeFqn, ImmutableSet.of(Permission.WRITE) );
                if( !modifiedWrite ){
                    addPermissionsForEntityType( role, entityTypeFqn, ImmutableSet.of(Permission.WRITE));
                    modifiedWrite = true;
                }
            }
            
            if( !modifiedRead && !modifiedWrite && Permission.canDoAction( propertyTypePermission, Permission.DISCOVER ) ){
                addPermissionsForPropertyTypeInEntityType( role, entityTypeFqn, propertyTypeFqn, ImmutableSet.of(Permission.DISCOVER) );
                addPermissionsForEntityType( role, entityTypeFqn, ImmutableSet.of(Permission.DISCOVER) );
            }
        });   
    }

    @Override
    public void inheritPermissionsFromPropertyType( FullQualifiedName entityTypeFqn, Set<FullQualifiedName> properties){
        ResultSet resultSet = tableManager.getAclsForEntityType( entityTypeFqn ); 
        Map<String, Integer> rolesToPermissions = EdmDetailsAdapter.aclAdapter( resultSet );
        for( Map.Entry<String, Integer> entry : rolesToPermissions.entrySet() ){
            Integer permission = entry.getValue();
            if( permission != 0){
                inheritPermissionsFromPropertyType( entry.getKey(), entityTypeFqn, properties);
                //TODO rethink if this is a good design choice; Cassandra seems to have automatic batch statement if the partition key is the same. For this reason you may want to do outer for loop with properties instead.
            }
        }   
    }

}
