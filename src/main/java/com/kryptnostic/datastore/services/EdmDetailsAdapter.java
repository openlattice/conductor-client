package com.kryptnostic.datastore.services;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.spark_project.guava.collect.Sets;

import com.dataloom.authorization.requests.Permission;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Preconditions;
import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.datastore.cassandra.CommonColumns;

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
}
