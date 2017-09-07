/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.dataloom.data.storage;

import static com.google.common.util.concurrent.Futures.transformAsync;

import com.codahale.metrics.annotation.Timed;
import com.dataloom.data.DatasourceManager;
import com.dataloom.data.EntityDatastore;
import com.dataloom.data.EntityKey;
import com.dataloom.data.EntityKeyIdService;
import com.dataloom.data.EntitySetData;
import com.dataloom.data.aggregators.EntitiesAggregator;
import com.dataloom.data.aggregators.EntityAggregator;
import com.dataloom.data.analytics.IncrementableWeightId;
import com.dataloom.data.events.EntityDataCreatedEvent;
import com.dataloom.data.events.EntityDataDeletedEvent;
import com.dataloom.data.hazelcast.DataKey;
import com.dataloom.data.hazelcast.Entities;
import com.dataloom.data.hazelcast.EntityKeyHazelcastStream;
import com.dataloom.data.hazelcast.EntitySetHazelcastStream;
import com.dataloom.data.hazelcast.EntitySets;
import com.dataloom.data.mapstores.DataMapstore;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.graph.core.LoomGraph;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.hazelcast.ListenableHazelcastFuture;
import com.dataloom.linking.HazelcastLinkingGraphs;
import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CassandraSerDesFactory;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraEntityDatastore implements EntityDatastore {
    private static final Logger logger = LoggerFactory
            .getLogger( CassandraEntityDatastore.class );

    private final Session           session;
    private final ObjectMapper      mapper;
    private final DatasourceManager dsm;

    private final HazelcastInstance         hazelcastInstance;
    private final IMap<DataKey, ByteBuffer> data;
    private final EntityKeyIdService        idService;
    private final ListeningExecutorService  executor;

    @Inject
    private EventBus eventBus;

    public CassandraEntityDatastore(
            Session session,
            HazelcastInstance hazelastInstance,
            ListeningExecutorService executor,
            ObjectMapper mapper,
            EntityKeyIdService idService,
            HazelcastLinkingGraphs linkingGraph,
            LoomGraph loomGraph,
            DatasourceManager dsm ) {
        this.session = session;
        this.mapper = mapper;
        this.dsm = dsm;
        CassandraTableBuilder dataTableDefinitions = Table.DATA.getBuilder();

        this.data = hazelastInstance.getMap( HazelcastMap.DATA.name() );
        this.idService = idService;
        this.hazelcastInstance = hazelastInstance;
        this.executor = executor;
    }

    @Override
    @Timed
    public EntitySetData<FullQualifiedName> getEntitySetData(
            UUID entitySetId,
            UUID syncId,
            LinkedHashSet<String> orderedPropertyNames,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        EntitySetHazelcastStream es = new EntitySetHazelcastStream( executor, hazelcastInstance, entitySetId, syncId );
        return new EntitySetData<>(
                orderedPropertyNames,
                StreamUtil.stream( es )
                        .map( e -> fromEntityBytes( e.getByteBuffers(), authorizedPropertyTypes ) )::iterator );
    }

    @Override
    @Timed
    public SetMultimap<FullQualifiedName, Object> getEntity(
            UUID entitySetId,
            UUID syncId,
            String entityId,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        SetMultimap<FullQualifiedName, Object> e = fromEntityBytes( entityId,
                data.aggregate( new EntityAggregator(), EntitySets.getEntity( entitySetId, syncId, entityId ) )
                        .getByteBuffers(),
                authorizedPropertyTypes );
        if ( e == null ) {
            return ImmutableSetMultimap.of();
        }
        return e;
    }

    @Override
    @Timed
    public Stream<SetMultimap<Object, Object>> getEntities(
            IncrementableWeightId[] utilizers,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        Predicate entitiesFilter = EntitySets.getEntities( Stream.of( utilizers )
                .map( IncrementableWeightId::getId )
                .toArray( UUID[]::new ) );
        Entities entities = data.aggregate( new EntitiesAggregator(), entitiesFilter );

        return Stream.of( utilizers )
                .map( weightedId -> {
                    UUID id = weightedId.getId();
                    SetMultimap<Object, Object> entity = untypedFromEntityBytes( id,
                            entities.get( id ),
                            authorizedPropertyTypes );
                    entity.put( "count", weightedId.getWeight() );
                    entity.put( "id", id.toString() );
                    return entity;
                } );
    }

    @Override
    @Timed
    public Stream<SetMultimap<Object, Object>> getEntities(
            Collection<UUID> ids,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        Predicate entitiesFilter = EntitySets.getEntities( ids );
        Entities entities = data.aggregate( new EntitiesAggregator(), entitiesFilter );

        return ids.stream()
                .map( id -> {
                    SetMultimap<Object, Object> entity = untypedFromEntityBytes( id,
                            entities.get( id ),
                            authorizedPropertyTypes );
                    entity.put( "id", id.toString() );
                    return entity;
                } );
    }

    @Override
    @Timed
    public Map<UUID, SetMultimap<FullQualifiedName, Object>> getEntitiesAcrossEntitySets(
            Map<UUID, UUID> entityKeyIdToEntitySetId,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesByEntitySet ) {
        Predicate entitiesFilter = EntitySets.getEntities( entityKeyIdToEntitySetId.keySet() );
        Entities entities = data.aggregate( new EntitiesAggregator(), entitiesFilter );

        return entityKeyIdToEntitySetId.entrySet().stream().collect( Collectors.toMap( Entry::getKey, entry -> {
            UUID entityKeyId = entry.getKey();
            UUID entitySetId = entry.getValue();
            SetMultimap<FullQualifiedName, Object> entity = fromEntityBytes( entityKeyId,
                    entities.get( entityKeyId ),
                    authorizedPropertyTypesByEntitySet.get( entitySetId ) );
            return entity;

        } ) );
    }

    @Override
    @Timed
    public SetMultimap<FullQualifiedName, Object> getEntity(
            UUID id,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        Predicate entitiesFilter = EntitySets.getEntity( id );
        Entities entities = data.aggregate( new EntitiesAggregator(), entitiesFilter );

        return fromEntityBytes( id, entities.get( id ), authorizedPropertyTypes );
    }

    public SetMultimap<FullQualifiedName, Object> fromEntityBytes(
            String entityId,
            SetMultimap<UUID, ByteBuffer> eb,
            Map<UUID, PropertyType> propertyType ) {
        SetMultimap<FullQualifiedName, Object> entityData = HashMultimap.create();

        eb.entries().forEach( prop -> {
            PropertyType pt = propertyType.get( prop.getKey() );
            if ( pt != null ) {
                entityData.put( pt.getType(), CassandraSerDesFactory.deserializeValue( mapper,
                        prop.getValue(),
                        pt.getDatatype(),
                        entityId ) );
            }
        } );
        return entityData;
    }

    public SetMultimap<FullQualifiedName, Object> fromEntityBytes(
            SetMultimap<UUID, ByteBuffer> rawData,
            Map<UUID, PropertyType> propertyType ) {
        SetMultimap<FullQualifiedName, Object> entityData = HashMultimap.create();

        rawData.entries().forEach( prop -> {
            PropertyType pt = propertyType.get( prop.getKey() );
            if ( pt != null ) {
                entityData.put( pt.getType(), CassandraSerDesFactory.deserializeValue( mapper,
                        prop.getValue(),
                        pt.getDatatype(),
                        "Entity id unavailable" ) );
            }
        } );
        return entityData;
    }

    public SetMultimap<Object, Object> untypedFromEntityBytes(
            SetMultimap<UUID, ByteBuffer> rawData,
            Map<UUID, PropertyType> propertyType ) {

        SetMultimap<Object, Object> entityData = HashMultimap
                .create( rawData.keySet().size(), rawData.size() / rawData.keySet().size() );

        rawData.entries().forEach( prop -> {
            PropertyType pt = propertyType.get( prop.getKey() );
            if ( pt != null ) {
                entityData.put( pt.getType(), CassandraSerDesFactory.deserializeValue( mapper,
                        prop.getValue(),
                        pt.getDatatype(),
                        "Entity id unavailable." ) );
            }
        } );

        return entityData;
    }

    public SetMultimap<FullQualifiedName, Object> fromEntityBytes(
            UUID id,
            SetMultimap<UUID, ByteBuffer> properties,
            Map<UUID, PropertyType> propertyType ) {
        SetMultimap<FullQualifiedName, Object> entityData = HashMultimap.create();
        if ( properties == null ) {
            logger.error( "Properties retreived from aggregator for id {} are null.", id );
            return HashMultimap.create();
        }
        properties.entries().forEach( prop -> {
            PropertyType pt = propertyType.get( prop.getKey() );
            if ( pt != null ) {
                entityData.put( pt.getType(), CassandraSerDesFactory.deserializeValue( mapper,
                        prop.getValue(),
                        pt.getDatatype(),
                        id::toString ) );
            }
        } );
        return entityData;
    }

    public SetMultimap<Object, Object> untypedFromEntityBytes(
            UUID id,
            SetMultimap<UUID, ByteBuffer> properties,
            Map<UUID, PropertyType> propertyType ) {
        if ( properties == null ) {
            logger.error( "Data for id {} was null", id );
            return HashMultimap.create();
        }
        SetMultimap<Object, Object> entityData = HashMultimap.create();

        properties.entries().forEach( prop -> {
            PropertyType pt = propertyType.get( prop.getKey() );
            if ( pt != null ) {
                entityData.put( pt.getType(), CassandraSerDesFactory.deserializeValue( mapper,
                        prop.getValue(),
                        pt.getDatatype(),
                        id::toString ) );
            }
        } );
        return entityData;
    }

    @Override
    @Timed
    public ListenableFuture<SetMultimap<FullQualifiedName, Object>> getEntityAsync(
            UUID entitySetId,
            UUID syncId,
            String entityId,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        return transformAsync( asyncLoadEntity( entitySetId, entityId, syncId, authorizedPropertyTypes.keySet() ),
                eb -> executor.submit( () -> fromEntityBytes( entityId, eb, authorizedPropertyTypes ) ),
                executor );
    }

    @Override
    public void updateEntity(
            EntityKey entityKey,
            SetMultimap<UUID, Object> entityDetails,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType ) {
        createData( entityKey.getEntitySetId(),
                entityKey.getSyncId(),
                authorizedPropertiesWithDataType,
                authorizedPropertiesWithDataType.keySet(),
                entityKey.getEntityId(),
                entityDetails );
    }

    @Override
    public Stream<ListenableFuture> updateEntityAsync(
            EntityKey entityKey,
            SetMultimap<UUID, Object> entityDetails,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType ) {
        return createDataAsync( entityKey.getEntitySetId(),
                entityKey.getSyncId(),
                authorizedPropertiesWithDataType,
                authorizedPropertiesWithDataType.keySet(),
                entityKey.getEntityId(),
                entityDetails );
    }

    public Stream<SetMultimap<UUID, Object>> getEntitySetDataIndexedById(
            UUID entitySetId,
            UUID syncId,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        return getRows( entitySetId, syncId, authorizedPropertyTypes.keySet() )
                .map( rs -> rowToEntityIndexedById( rs, authorizedPropertyTypes ) );
    }

    public SetMultimap<FullQualifiedName, Object> rowToEntity(
            ResultSet rs,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        return RowAdapters.entity( rs, authorizedPropertyTypes, mapper );
    }

    public SetMultimap<UUID, Object> rowToEntityIndexedById(
            ResultSet rs,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        return RowAdapters.entityIndexedById( rs, authorizedPropertyTypes, mapper );
    }

    public SetMultimap<UUID, Object> rowToEntityIndexedById(
            EntityBytes eb,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        return RowAdapters.entityIndexedById( eb, authorizedPropertyTypes, mapper );
    }

    public SetMultimap<UUID, Object> rowToEntityIndexedById(
            String entityId,
            SetMultimap<UUID, ByteBuffer> eb,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        return RowAdapters.entityIndexedById( entityId, eb, authorizedPropertyTypes, mapper );
    }

    private Stream<EntityBytes> getRows(
            UUID entitySetId,
            UUID syncId,
            Set<UUID> authorizedProperties ) {
        // If syncId is not specified, retrieve latest snapshot of entity
        final UUID finalSyncId;
        if ( syncId == null ) {
            finalSyncId = dsm.getCurrentSyncId( entitySetId );
        } else {
            finalSyncId = syncId;
        }

        // return StreamUtil
        // .stream( asyncLoadEntitySet( entitySetId, finalSyncId, authorizedProperties ).getUninterruptibly() )
        // .map( row -> RowAdapters.entity( ) );
        return getEntityIds( entitySetId, finalSyncId )
                .map( entityId -> asyncLoadEntity( entitySetId, entityId, finalSyncId ) )
                .map( rsf -> {
                    Stopwatch w = Stopwatch.createStarted();
                    // ResultSet rs = rsf.getUninterruptibly();
                    EntityBytes eb = StreamUtil.safeGet( rsf );
                    logger.info( "Load entity took: {}", w.elapsed( TimeUnit.MILLISECONDS ) );
                    return eb;
                } );
        // .map( ResultSetFuture::getUninterruptibly );
    }

    public Stream<String> getEntityIds( UUID entitySetId, UUID syncId ) {
        return getEntityKeysForEntitySet( entitySetId, syncId ).map( EntityKey::getEntityId );
        // BoundStatement boundEntityIdsQuery = entityIdsQuery.bind()
        // .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
        // .setUUID( CommonColumns.SYNCID.cql(), syncId );
        // ResultSet entityIds = session.execute( boundEntityIdsQuery );
        // return StreamUtil
        // .stream( entityIds )
        // .parallel()
        // .unordered()
        // .map( RowAdapters::entityId )
        // .distinct()
        // .filter( StringUtils::isNotBlank );
    }

    // public ResultSetFuture asyncLoadEntitySet( UUID entitySetId, UUID syncId, Set<UUID> authorizedProperties ) {
    // return session.executeAsync( entitySetQuery.bind()
    // .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
    // .setUUID( CommonColumns.SYNCID.cql(), syncId )
    // .setSet( CommonColumns.PRINCIPAL_TYPE.cql(), authorizedProperties ) );
    // }

    @Override
    public ListenableFuture<SetMultimap<UUID, ByteBuffer>> asyncLoadEntity(
            UUID entitySetId,
            String entityId,
            UUID syncId,
            Set<UUID> authorizedProperties ) {

        ListenableFuture<SetMultimap<UUID, ByteBuffer>> f = executor.submit( () -> {
            SetMultimap<UUID, ByteBuffer> byteBuffers = data.aggregate( new EntityAggregator(),
                    EntitySets.getEntity(
                            entitySetId,
                            syncId,
                            entityId,
                            authorizedProperties ) )
                    .getByteBuffers();
            return byteBuffers;
        } );
        return f;
        // return new ListenableHazelcastFuture( data.getAsync( id ) );

        // return session.executeAsync( entityQuery.bind()
        // .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
        // .setString( CommonColumns.ENTITYID.cql(), entityId )
        // .setSet( CommonColumns.PROPERTY_TYPE_ID.cql(), authorizedProperties )
        // .setUUID( CommonColumns.SYNCID.cql(), syncId ) );
    }

    @Override
    public SetMultimap<UUID, Object> loadEntities(
            Map<UUID, Set<UUID>> authorizedPropertyTypesForEntity,
            Map<UUID, PropertyType> propertyTypesById,
            Set<UUID> propertyTypesToPopulate ) {

        Predicate entitiesFilter = EntitySets
                .getEntities( authorizedPropertyTypesForEntity.keySet().toArray( new UUID[ 0 ] ) );
        Entities entities = data.aggregate( new EntitiesAggregator(), entitiesFilter );

        SetMultimap<UUID, ByteBuffer> mergedEntity = HashMultimap.create();

        entities.entrySet().forEach( entityDetails -> {
            Set<UUID> authorizedPropertyTypes = authorizedPropertyTypesForEntity.get( entityDetails.getKey() );
            mergedEntity.putAll(
                    Multimaps.filterKeys( entityDetails.getValue(), key -> authorizedPropertyTypes.contains( key ) ) );
        } );

        return RowAdapters.entityIndexedById( UUID.randomUUID().toString(),
                mergedEntity,
                propertyTypesById,
                propertyTypesToPopulate,
                mapper );
    }

    /*
     * Warning: this loads ALL the properties of the entity, authorized or not.
     */
    @Override
    public ListenableFuture<EntityBytes> asyncLoadEntity(
            UUID entitySetId,
            String entityId,
            UUID syncId ) {

        // UUID id = idService.getEntityKeyId( new EntityKey( entitySetId, entityId, syncId ) );
        return executor.submit( () -> {
            EntityKey ek = new EntityKey( entitySetId, entityId, syncId );
            SetMultimap<UUID, byte[]> mm = HashMultimap.create();
            EntityBytes eb = new EntityBytes( ek, mm );
            data.entrySet( EntitySets.getEntity( entitySetId, syncId, entityId ) )
                    .forEach( e -> mm.put( e.getKey().getPropertyTypeId(), e.getValue().array() ) );
            return eb;
        } );
    }

    @Deprecated
    public void createEntityData(
            UUID entitySetId,
            UUID syncId,
            Map<String, SetMultimap<UUID, Object>> entities,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType ) {
        Set<UUID> authorizedProperties = authorizedPropertiesWithDataType.keySet();

        entities.entrySet().stream().flatMap( entity -> createDataAsync( entitySetId,
                syncId,
                authorizedPropertiesWithDataType,
                authorizedProperties,
                entity.getKey(),
                entity.getValue() ) )
                .forEach( StreamUtil::getUninterruptibly );
    }

    @Timed
    public void createData(
            UUID entitySetId,
            UUID syncId,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType,
            Set<UUID> authorizedProperties,
            String entityId,
            SetMultimap<UUID, Object> entityDetails ) {
        createDataAsync(
                entitySetId,
                syncId,
                authorizedPropertiesWithDataType,
                authorizedProperties,
                entityId,
                entityDetails ).forEach( StreamUtil::getUninterruptibly );
    }

    @Timed
    public Stream<ListenableFuture> createDataAsync(
            UUID entitySetId,
            UUID syncId,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType,
            Set<UUID> authorizedProperties,
            String entityId,
            SetMultimap<UUID, Object> entityDetails ) {

        // does not write the row if some property values that user is trying to write to are not authorized.
        if ( !authorizedProperties.containsAll( entityDetails.keySet() ) ) {
            logger.error( "Entity {} not written because the following properties are not authorized: {}",
                    entityId,
                    Sets.difference( entityDetails.keySet(), authorizedProperties ) );
            return Stream.empty();
        }

        SetMultimap<UUID, Object> normalizedPropertyValues = null;
        try {
            normalizedPropertyValues = CassandraSerDesFactory.validateFormatAndNormalize( entityDetails,
                    authorizedPropertiesWithDataType );
        } catch ( Exception e ) {
            logger.error( "Entity {} not written because some property values are of invalid format.",
                    entityId,
                    e );
            return Stream.empty();

        }

        EntityKey ek = new EntityKey( entitySetId, entityId, syncId );
        UUID id = idService.getEntityKeyId( ek );
        Stream<ListenableFuture> futures =
                normalizedPropertyValues
                        .entries().stream()
                        .map( entry -> {
                            UUID propertyTypeId = entry.getKey();
                            EdmPrimitiveTypeKind datatype = authorizedPropertiesWithDataType.get( propertyTypeId );
                            ByteBuffer buffer = CassandraSerDesFactory.serializeValue(
                                    mapper,
                                    entry.getValue(),
                                    datatype,
                                    entityId );
                            return data.setAsync( new DataKey(
                                    id,
                                    entitySetId,
                                    syncId,
                                    entityId,
                                    propertyTypeId,
                                    DataMapstore.hf.hashBytes( buffer.array() ).asBytes() ), buffer );
                        } )
                        .map( ListenableHazelcastFuture::new );

        Map<UUID, Object> normalizedPropertyValuesAsMap = normalizedPropertyValues
                .asMap().entrySet().stream()
                .filter( entry -> !authorizedPropertiesWithDataType.get( entry.getKey() )
                        .equals( EdmPrimitiveTypeKind.Binary ) )
                .collect( Collectors.toMap( e -> e.getKey(), e -> new HashSet<>( e.getValue() ) ) );

        eventBus.post( new EntityDataCreatedEvent(
                entitySetId,
                Optional.of( syncId ),
                entityId,
                normalizedPropertyValuesAsMap ) );

        return futures;
    }

    /**
     * Delete data of an entity set across ALL sync Ids.
     * <p>
     * Note: this is currently only used when deleting an entity set, which takes care of deleting the data in
     * elasticsearch. If this is ever called without deleting the entity set, logic must be added to delete the data
     * from elasticsearch.
     */
    @SuppressFBWarnings(
            value = "UC_USELESS_OBJECT",
            justification = "results Object is used to execute deletes in batches" )
    public void deleteEntitySetData( UUID entitySetId ) {
        logger.info( "Deleting data of entity set: {}", entitySetId );

        try {
            asyncDeleteEntitySet( entitySetId ).get();
            logger.info( "Finished deletion of entity set data: {}", entitySetId );
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Unable to delete entity set {}", entitySetId );
        }

    }

    public ListenableFuture<?> asyncDeleteEntitySet( UUID entitySetId ) {
        return executor.submit( () -> StreamUtil.stream( dsm.getAllSyncIds( entitySetId ) )
                .parallel()
                .flatMap( syncId -> getEntityKeysForEntitySet( entitySetId, syncId ) )
                .forEach( data::delete ) );

        // return StreamUtil.stream( dsm.getAllSyncIds( entitySetId ) )
        // .parallel()
        // .flatMap( syncId ->
        // PARTITION_INDEXES
        // .stream()
        // .map( partitionIndex -> deleteEntitySetQuery.bind()
        // .setByte( CommonColumns.PARTITION_INDEX.cql(), partitionIndex )
        // .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
        // .setUUID( CommonColumns.SYNCID.cql(), syncId ) ) )
        // .map( session::executeAsync );
    }

    public ListenableFuture<?> asyncDeleteEntity( UUID entitySetId, String entityId, UUID syncId ) {
        // UUID id = idService.getEntityKeyId( new EntityKey( entitySetId, entityId, syncId ) );
        return executor.submit( () -> data.removeAll( EntitySets.getEntity( entitySetId, syncId, entityId ) ) );
        // return session.executeAsync( deleteEntityQuery.bind()
        // .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
        // .setString( CommonColumns.ENTITYID.cql(), entityId )
        // .setUUID( CommonColumns.SYNCID.cql(), syncId ) );
    }

    @Override
    public void deleteEntity( EntityKey entityKey ) {
        try {
            asyncDeleteEntity( entityKey.getEntitySetId(), entityKey.getEntityId(), entityKey.getSyncId() ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Unable to delete entity {}", entityKey );
        }

        eventBus.post( new EntityDataDeletedEvent(
                entityKey.getEntitySetId(),
                entityKey.getEntityId(),
                Optional.of( entityKey.getSyncId() ) ) );
    }

    @Override
    public Stream<EntityKey> getEntityKeysForEntitySet( UUID entitySetId, UUID syncId ) {
        EntityKeyHazelcastStream es = new EntityKeyHazelcastStream( executor,
                hazelcastInstance,
                entitySetId,
                syncId );
        return StreamUtil.stream( es );
    }
}
