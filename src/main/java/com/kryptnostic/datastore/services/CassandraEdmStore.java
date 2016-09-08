package com.kryptnostic.datastore.services;

import java.util.Set;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Query;
import com.google.common.util.concurrent.ListenableFuture;
import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.datastore.cassandra.Queries;

@Accessor
public interface CassandraEdmStore {
    @Query( Queries.GET_ALL_ENTITY_TYPES_QUERY )
    public Result<EntityType> getEntityTypes();

    @Query( Queries.GET_ALL_ENTITY_TYPES_QUERY )
    public ListenableFuture<Result<EntityType>> getObjectTypesAsync();

    @Query( Queries.GET_ALL_PROPERTY_TYPES_IN_NAMESPACE )
    public Result<PropertyType> getPropertyTypesInNamespace( String namespace );

    @Query( Queries.CREATE_ENTITY_TYPE_IF_NOT_EXISTS )
    public ResultSet createEntityTypeIfNotExists(
            String namespace,
            String type,
            String typename,
            Set<FullQualifiedName> key,
            Set<FullQualifiedName> properties );

    @Query( Queries.CREATE_PROPERTY_TYPE_IF_NOT_EXISTS )
    public ResultSet createPropertyTypeIfNotExists(
            String namespace,
            String type,
            String typename,
            EdmPrimitiveTypeKind datatype,
            long multiplicity );

    @Query( Queries.CREATE_ENTITY_SET_IF_NOT_EXISTS )
    public ResultSet createEntitySetIfNotExists( 
            String typename,
            FullQualifiedName type, 
            String name, 
            String title);

    @Query( Queries.GET_ENTITY_SET_BY_NAME )
    public EntitySet getEntitySet( String name );

    @Query( Queries.GET_ALL_ENTITY_SETS )
    public Result<EntitySet> getEntitySets();
}
