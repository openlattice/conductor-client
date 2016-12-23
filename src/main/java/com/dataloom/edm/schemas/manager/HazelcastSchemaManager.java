package com.dataloom.edm.schemas.manager;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.edm.internal.AbstractSchemaAssociatedSecurableType;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.edm.internal.Schema;
import com.dataloom.edm.schemas.SchemaQueryService;
import com.dataloom.edm.schemas.processors.AddSchemasToType;
import com.dataloom.edm.schemas.processors.RemoveSchemasFromType;
import com.dataloom.edm.schemas.processors.SchemaMerger;
import com.dataloom.edm.schemas.processors.SchemaRemover;
import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class HazelcastSchemaManager {
    // map( namespace -> set<name> )
    private final IMap<String, Set<String>>   schemas;
    private final IMap<UUID, PropertyType>    propertyTypes;
    private final IMap<UUID, EntityType>      entityTypes;

    private final SchemaQueryService schemaQueryService;

    public HazelcastSchemaManager(
            String keyspace,
            HazelcastInstance hazelcastInstance,
            SchemaQueryService schemaQueryService ) {
        this.schemas = checkNotNull( hazelcastInstance.getMap( HazelcastMap.SCHEMAS.name() ) );
        this.propertyTypes = checkNotNull( hazelcastInstance.getMap( HazelcastMap.PROPERTY_TYPES.name() ) );
        this.entityTypes = checkNotNull( hazelcastInstance.getMap( HazelcastMap.ENTITY_TYPES.name() ) );
        this.schemaQueryService = schemaQueryService;
    }

    public void createOrUpdateSchemas( Schema... schemas ) {
        upsertSchemas( ImmutableSet.copyOf( Iterables.transform( Arrays.asList( schemas ), Schema::getFqn ) ) );
    }

    public void addPropertyTypesToSchema( Set<UUID> propertyTypeUuids, FullQualifiedName schemaName ) {
        addTypesToSchema( propertyTypes, propertyTypeUuids, ImmutableList.of( schemaName ) );
    }

    public void addEntityTypesToSchema( Set<UUID> entityTypeUuids, FullQualifiedName schemaName ) {
        addTypesToSchema( entityTypes, entityTypeUuids, ImmutableList.of( schemaName ) );
    }

    public void removePropertyTypesFromSchema( Set<UUID> propertyTypeUuids, FullQualifiedName schemaName ) {
        removeTypesFromSchema( propertyTypes, propertyTypeUuids, ImmutableList.of( schemaName ) );
    }

    public void removeEntityTypesFromSchema( Set<UUID> entityTypeUuids, FullQualifiedName schemaName ) {
        removeTypesFromSchema( entityTypes, entityTypeUuids, ImmutableList.of( schemaName ) );
    }

    public Consumer<FullQualifiedName> propertyTypesSchemaAdder( Set<UUID> propertyTypeUuids ) {
        return schema -> addPropertyTypesToSchema( propertyTypeUuids, schema );
    }

    public Consumer<FullQualifiedName> entityTypesSchemaAdder( Set<UUID> entityTypeUuids ) {
        return schema -> addPropertyTypesToSchema( entityTypeUuids, schema );
    }

    public Consumer<FullQualifiedName> propertyTypesSchemaAdder( UUID... propertyTypeUuids ) {
        return schema -> addPropertyTypesToSchema( ImmutableSet.copyOf( propertyTypeUuids ), schema );
    }

    public Consumer<FullQualifiedName> entityTypesSchemaAdder( UUID... entityTypeUuids ) {
        return schema -> addEntityTypesToSchema( ImmutableSet.copyOf( entityTypeUuids ), schema );
    }

    public Consumer<FullQualifiedName> propertyTypesSchemaRemover( Set<UUID> propertyTypeUuids ) {
        return schema -> removePropertyTypesFromSchema( propertyTypeUuids, schema );
    }

    public Consumer<FullQualifiedName> entityTypesSchemaRemover( Set<UUID> entityTypeUuids ) {
        return schema -> removePropertyTypesFromSchema( entityTypeUuids, schema );
    }

    public Consumer<FullQualifiedName> propertyTypesSchemaRemover( UUID... propertyTypeUuids ) {
        return schema -> removePropertyTypesFromSchema( ImmutableSet.copyOf( propertyTypeUuids ), schema );
    }

    public Consumer<FullQualifiedName> entityTypesSchemaRemover( UUID... entityTypeUuids ) {
        return schema -> removeEntityTypesFromSchema( ImmutableSet.copyOf( entityTypeUuids ), schema );
    }

    private <T extends AbstractSchemaAssociatedSecurableType> void addTypesToSchema(
            IMap<UUID, T> m,
            Set<UUID> typeUuids,
            Collection<FullQualifiedName> schemaNames ) {
        AddSchemasToType ep = new AddSchemasToType( schemaNames );
        m.executeOnKeys( typeUuids, ep );
    }

    private <T extends AbstractSchemaAssociatedSecurableType> void removeTypesFromSchema(
            IMap<UUID, T> m,
            Set<UUID> typeUuids,
            Collection<FullQualifiedName> schemaNames ) {
        RemoveSchemasFromType ep = new RemoveSchemasFromType( schemaNames );
        m.executeOnKeys( typeUuids, ep );
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

    public Schema getSchema( String namespace, String name ) {
        FullQualifiedName schemaName = new FullQualifiedName( namespace, name );
        return new Schema(
                schemaName,
                schemaQueryService.getAllEntityTypesInSchema( schemaName ),
                schemaQueryService.getAllPropertyTypesInSchema( schemaName ) );
    }

    public Iterable<Schema> getAllSchemas() {
        return null;
    }

    public Iterable<Schema> getSchemasInNamespace( String namespace ) {
        return null;
    }
}
