package com.kryptnostic.datastore.services;

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.google.common.base.Preconditions;
import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.datastore.Permission;

/**
 * Static factory that adds details to Edm objects
 * @author Ho Chung
 *
 */
public class EdmDetailsAdapter {

	private void EdmDetailsAdapterFactory(){}
	
	public static EntitySet setEntitySetTypename( CassandraTableManager ctb, EntitySet es ){
		Preconditions.checkNotNull( es.getTypename(), "Entity set has no associated entity type.");
		return es.setType( ctb.getEntityTypeForTypename( es.getTypename() ) );
	}
	
	public static EntityType setViewableDetails( CassandraTableManager ctb, PermissionsService ps, EntityType entityType){
		//Set viewable properties of entity type
		Set<FullQualifiedName> viewableProperties = entityType.getProperties().stream()
				.filter( propertyTypeFqn -> ps.checkUserHasPermissionsOnPropertyType( propertyTypeFqn, Permission.DISCOVER ) )
				.collect( Collectors.toSet() );
		
		entityType.setViewableProperties( viewableProperties );
		return entityType;
	}

}
