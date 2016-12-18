package com.kryptnostic.datastore.services;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.edm.internal.Schema;
import com.dataloom.edm.requests.GetSchemasRequest.TypeDetails;
import com.dataloom.edm.schemas.processors.AddSchemasToType;
import com.dataloom.edm.schemas.processors.SchemaMerger;
import com.dataloom.edm.schemas.processors.SchemaRemover;
import com.dataloom.hazelcast.HazelcastMap;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;

public class CassandraSchemaManager {
    // map( string -> string )
    private final IMap<String, Set<String>>             schemas;
    private final IMap<FullQualifiedName, PropertyType> propertyTypes;
    private final IMap<FullQualifiedName, EntityType>   entityTypes;

    private final Session                               session;
    private PreparedStatement                           propertyTypesInSchemaQuery;
    private PreparedStatement                           entityTypesInSchemaQuery;

    public CassandraSchemaManager( HazelcastInstance hazelcastInstance, Session session, String keyspace ) {
        this.schemas = checkNotNull( hazelcastInstance.getMap( HazelcastMap.SCHEMAS.name() ) );
        this.session = checkNotNull( session );
        this.propertyTypes = checkNotNull( hazelcastInstance.getMap( HazelcastMap.PROPERTY_TYPES.name() ) );
        this.entityTypes = checkNotNull( hazelcastInstance.getMap( HazelcastMap.ENTITY_TYPES.name() ) );
        propertyTypesInSchemaQuery = session.prepare( getPropertyTypesInSchema( keyspace ) );
        entityTypesInSchemaQuery = session.prepare( getEntityTypesInSchema( keyspace ) );
    }

    public void addSchemasToEntityTypes( SetMultimap<FullQualifiedName, FullQualifiedName> entityTypesToSchemas ) {
        entityTypesToSchemas.asMap().entrySet().forEach(
                entry -> entityTypes.submitToKey( entry.getKey(),
                        new AddSchemasToType<EntityType>( entry.getValue() ) ) );
    }

    public void addSchemasToPropertyTypes( SetMultimap<FullQualifiedName, FullQualifiedName> propertyTypesToSchemas ) {
        propertyTypesToSchemas.asMap().entrySet().forEach(
                entry -> propertyTypes.submitToKey( entry.getKey(),
                        new AddSchemasToType<PropertyType>( entry.getValue() ) ) );
    }

    public void removeSchemasFromEntityTypes( SetMultimap<FullQualifiedName, FullQualifiedName> entityTypesToSchemas ) {
        entityTypesToSchemas.asMap().entrySet().forEach(
                entry -> entityTypes.submitToKey( entry.getKey(),
                        new AddSchemasToType<EntityType>( entry.getValue() ) ) );
    }

    public void removeSchemasFromPropertyTypes(
            SetMultimap<FullQualifiedName, FullQualifiedName> propertyTypesToSchemas ) {
        propertyTypesToSchemas.asMap().entrySet().forEach(
                entry -> propertyTypes.submitToKey( entry.getKey(),
                        new AddSchemasToType<PropertyType>( entry.getValue() ) ) );
    }

    public void upsertSchemas( Set<FullQualifiedName> schemaNames ) {
        Set<String> namespaces = schemaNames.stream()
                .map( FullQualifiedName::getNamespace )
                .collect( Collectors.toSet() );
        Set<String> names = schemaNames.stream()
                .map( FullQualifiedName::getName )
                .collect( Collectors.toSet() );
        schemas.executeOnKeys( namespaces, new SchemaMerger( names ) );
    }

    public void deleteSchema( Set<FullQualifiedName> schemaNames ) {
        Set<String> namespaces = schemaNames.stream()
                .map( FullQualifiedName::getNamespace )
                .collect( Collectors.toSet() );
        Set<String> names = schemaNames.stream()
                .map( FullQualifiedName::getName )
                .collect( Collectors.toSet() );
        schemas.executeOnKeys( namespaces, new SchemaRemover( names ) );
    }

    public Schema getSchema( String namespace, String name, Set<TypeDetails> requestedDetails ) {
        FullQualifiedName schemaName = new FullQualifiedName( namespace, name );
        return new Schema(
                schemaName,
                getAllEntityTypesInSchema( schemaName ),
                getAllPropertyTypesInSchema( schemaName ) );
    }

    private Set<PropertyType> getAllPropertyTypesInSchema( FullQualifiedName schemaName ) {
        ResultSet propertyTypes = session.execute(
                propertyTypesInSchemaQuery.bind()
                        .setString( CommonColumns.NAMESPACE.cql(), schemaName.getNamespace() )
                        .setString( CommonColumns.NAME.cql(), schemaName.getName() ) );
        return ImmutableSet.copyOf( Iterables.transform( propertyTypes, RowAdapters::propertyType ) );
    }

    private Set<EntityType> getAllEntityTypesInSchema( FullQualifiedName schemaName ) {
        ResultSet propertyTypes = session.execute(
                entityTypesInSchemaQuery.bind()
                        .setString( CommonColumns.NAMESPACE.cql(), schemaName.getNamespace() )
                        .setString( CommonColumns.NAME.cql(), schemaName.getName() ) );
        return ImmutableSet.copyOf( Iterables.transform( propertyTypes, RowAdapters::entityType ) );
    }

    private static RegularStatement getPropertyTypesInSchema( String keyspace ) {
        return QueryBuilder.select().all().from( keyspace, Tables.PROPERTY_TYPES.getName() )
                .where( QueryBuilder.contains( CommonColumns.SCHEMAS.cql(), CommonColumns.SCHEMAS.bindMarker() ) );
    }

    private static RegularStatement getEntityTypesInSchema( String keyspace ) {
        return QueryBuilder.select().all().from( keyspace, Tables.ENTITY_TYPES.getName() )
                .where( QueryBuilder.contains( CommonColumns.SCHEMAS.cql(), CommonColumns.SCHEMAS.bindMarker() ) );
    }
}
