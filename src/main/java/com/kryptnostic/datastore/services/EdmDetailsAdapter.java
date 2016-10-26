package com.kryptnostic.datastore.services;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.spark_project.guava.collect.Sets;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Preconditions;
import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.datastore.Permission;
import com.kryptnostic.datastore.cassandra.CommonColumns;

/**
 * Static factory that adds details to Edm objects
 * @author Ho Chung
 *
 */
public class EdmDetailsAdapter {

	/** 
	 * Being of debug
	 */
    private static String username;
	private static Set<String>                   currentRoles;
	public static void setCurrentUserForDebug( String username, Set<String> roles ){
	    EdmDetailsAdapter.username = username;
		EdmDetailsAdapter.currentRoles = roles;
	}
	/**
	 * End of debug
	 */
	
	private void EdmDetailsAdapterFactory(){}
	
	public static EntitySet setEntitySetTypename( CassandraTableManager ctb, EntitySet es ){
		Preconditions.checkNotNull( es.getTypename(), "Entity set has no associated entity type.");
		return es.setType( ctb.getEntityTypeForTypename( es.getTypename() ) );
	}
	
	public static EntityType setViewableDetails( CassandraTableManager ctb, ActionAuthorizationService authzService, EntityType entityType){
		//Set viewable properties of entity type
		Set<FullQualifiedName> viewableProperties = entityType.getProperties().stream()
				.filter( propertyTypeFqn -> authzService.getPropertyType( currentRoles, propertyTypeFqn ) )
				.collect( Collectors.toSet() );
		entityType.setViewableProperties( viewableProperties );
		
	    //Set viewable key of entity type
		Set<FullQualifiedName> viewableKey = Sets.intersection( entityType.getKey(), viewableProperties );
		entityType.setViewableKey( viewableKey );
		
		return entityType;
	}

	public static Map<String, Integer> aclAdapter( ResultSet resultSet ){
        Map<String, Integer> rolesToPermissions = new HashMap<>();
        for(Row row: resultSet){
            rolesToPermissions.put( row.getString( CommonColumns.ROLE.cql() ), row.getInt( CommonColumns.PERMISSIONS.cql() ) );
        }
        return rolesToPermissions;
	}
}
