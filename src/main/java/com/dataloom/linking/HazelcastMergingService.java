package com.dataloom.linking;

import com.dataloom.blocking.GraphEntityPair;
import com.dataloom.blocking.LinkingEntity;
import com.dataloom.data.DataGraphService;
import com.dataloom.data.EntityKey;
import com.dataloom.data.EntityKeyIdService;
import com.dataloom.data.aggregators.EntitiesAggregator;
import com.dataloom.data.hazelcast.DataKey;
import com.dataloom.data.hazelcast.Entities;
import com.dataloom.data.hazelcast.EntitySets;
import com.dataloom.data.ids.HazelcastEntityKeyIdService;
import com.dataloom.data.mapstores.DataMapstore;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.graph.core.LoomGraph;
import com.dataloom.graph.edge.LoomEdge;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.hazelcast.ListenableHazelcastFuture;
import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.kryptnostic.conductor.rpc.ConductorElasticsearchApi;
import com.kryptnostic.datastore.cassandra.CassandraSerDesFactory;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.datastore.util.Util;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HazelcastMergingService {
    private static final Logger logger = LoggerFactory.getLogger( HazelcastMergingService.class );
    private final EntityKeyIdService                   ekIds;
    @Inject
    private       ConductorElasticsearchApi            elasticsearchApi;
    private       IMap<GraphEntityPair, LinkingEntity> linkingEntities;
    private       IMap<DataKey, ByteBuffer>            data;
    private       IMap<LinkingVertexKey, UUID>         newIds;
    private       IMap<EntityKey, UUID>                ids;
    private       LoomGraph                            graph;
    private       HazelcastInstance                    hazelcastInstance;
    private       ObjectMapper                         mapper;

    public HazelcastMergingService( HazelcastInstance hazelcastInstance, ListeningExecutorService executor ) {
        this.data = hazelcastInstance.getMap( HazelcastMap.DATA.name() );
        this.newIds = hazelcastInstance.getMap( HazelcastMap.VERTEX_IDS_AFTER_LINKING.name() );
        this.mapper = ObjectMappers.getJsonMapper();

        this.ids = hazelcastInstance.getMap( HazelcastMap.IDS.name() );
        this.ekIds = new HazelcastEntityKeyIdService( hazelcastInstance, executor );
        this.graph = new LoomGraph( executor, hazelcastInstance );
        this.hazelcastInstance = hazelcastInstance;

    }

    private SetMultimap<UUID, Object> computeMergedEntity(
            Set<UUID> entityKeyIds,
            Map<UUID, Set<UUID>> propertyTypeIdsByEntitySet,
            Map<UUID, PropertyType> propertyTypesById,
            Set<UUID> propertyTypesToPopulate ) {

        Map<UUID, Set<UUID>> authorizedPropertyTypesForEntity = ekIds.getEntityKeyEntries( entityKeyIds )
                .stream()
                .collect( Collectors.toMap( Entry::getValue,
                        entry -> propertyTypeIdsByEntitySet.get( entry.getKey().getEntitySetId() ) ) );

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

    @Async
    public void mergeEntity(
            Set<UUID> entityKeyIds, UUID graphId,
            UUID syncId,
            Map<UUID, Set<UUID>> propertyTypeIdsByEntitySet,
            Map<UUID, PropertyType> propertyTypesById,
            Set<UUID> propertyTypesToPopulate,
            Map<UUID, EdmPrimitiveTypeKind> propertyTypesWithDatatype ) {
        SetMultimap<UUID, Object> mergedEntity = computeMergedEntity( entityKeyIds,
                propertyTypeIdsByEntitySet,
                propertyTypesById,
                propertyTypesToPopulate );

        String entityId = UUID.randomUUID().toString();

        // create merged entity, in particular get back the entity key id for the new entity
        UUID mergedEntityKeyId;
        try {
            mergedEntityKeyId = createEntity( entityId, mergedEntity, graphId, syncId, propertyTypesWithDatatype );

            // write to a lookup table from old entity key id to new, merged entity key id
            entityKeyIds.forEach( oldId -> newIds.put( new LinkingVertexKey( graphId, oldId ), mergedEntityKeyId ) );

        } catch ( ExecutionException | InterruptedException e ) {
            logger.error( "Failed to create linked entity" );
        }

        hazelcastInstance.getCountDownLatch( graphId.toString() ).countDown();
    }

    public UUID createEntity(
            String entityId,
            SetMultimap<UUID, Object> entityDetails,
            UUID graphId,
            UUID syncId,
            Map<UUID, EdmPrimitiveTypeKind> propertyTypesWithDatatype )
            throws ExecutionException, InterruptedException {

        final EntityKey key = new EntityKey( graphId, entityId, syncId );
        final ListenableFuture reservationAndVertex = new ListenableHazelcastFuture<>( ids.getAsync( key ) );
        final Stream<ListenableFuture> writes = createDataAsync( entityId,
                graphId,
                syncId,
                entityDetails,
                propertyTypesWithDatatype );
        Stream.concat( Stream.of( reservationAndVertex ), writes ).forEach( DataGraphService::tryGetAndLogErrors );
        return ids.get( key );
    }

    private Stream<ListenableFuture> createDataAsync(
            String entityId,
            UUID graphId,
            UUID syncId,
            SetMultimap<UUID, Object> entityDetails,
            Map<UUID, EdmPrimitiveTypeKind> propertyTypesWithDatatype ) {

        Set<UUID> authorizedProperties = propertyTypesWithDatatype.keySet();
        // does not write the row if some property values that user is trying to write to are not authorized.
        if ( !authorizedProperties.containsAll( entityDetails.keySet() ) ) {
            logger.error( "Entity {} not written because the following properties are not authorized: {}",
                    entityId,
                    Sets.difference( entityDetails.keySet(), authorizedProperties ) );
            return Stream.empty();
        }

        SetMultimap<UUID, Object> normalizedPropertyValues;
        try {
            normalizedPropertyValues = CassandraSerDesFactory.validateFormatAndNormalize( entityDetails,
                    propertyTypesWithDatatype );
        } catch ( Exception e ) {
            logger.error( "Entity {} not written because some property values are of invalid format.",
                    entityId,
                    e );
            return Stream.empty();

        }

        EntityKey ek = new EntityKey( graphId, entityId, syncId );
        UUID id = ids.get( ek );
        Stream<ListenableFuture> futures =
                normalizedPropertyValues
                        .entries().stream()
                        .map( entry -> {
                            UUID propertyTypeId = entry.getKey();
                            EdmPrimitiveTypeKind datatype = propertyTypesWithDatatype
                                    .get( propertyTypeId );
                            ByteBuffer buffer = CassandraSerDesFactory.serializeValue(
                                    mapper,
                                    entry.getValue(),
                                    datatype,
                                    entityId );
                            return data.setAsync( new DataKey(
                                    id,
                                    graphId,
                                    syncId,
                                    entityId,
                                    propertyTypeId,
                                    DataMapstore.hf.hashBytes( buffer.array() ).asBytes() ), buffer );
                        } )
                        .map( ListenableHazelcastFuture::new );

        propertyTypesWithDatatype.entrySet().forEach( entry -> {
            if ( entry.getValue().equals( EdmPrimitiveTypeKind.Binary ) )
                normalizedPropertyValues.removeAll( entry.getKey() );
        } );

        elasticsearchApi.updateEntityData( graphId,
                syncId,
                entityId,
                normalizedPropertyValues );

        return futures;
    }

    public UUID getMergedId( UUID graphId, UUID oldId ) {
        return Util.getSafely( newIds, new LinkingVertexKey( graphId, oldId ) );
    }

    @Async
    public void mergeEdgeAsync( UUID linkedEntitySetId, UUID syncId, LoomEdge edge ) {
        UUID srcEntitySetId = edge.getSrcSetId();
        UUID srcSyncId = edge.getSrcSyncId();
        UUID dstEntitySetId = edge.getDstSetId();
        UUID dstSyncId = edge.getDstSyncId();
        UUID edgeEntitySetId = edge.getEdgeSetId();

        UUID srcId = edge.getKey().getSrcEntityKeyId();
        UUID dstId = edge.getKey().getDstEntityKeyId();
        UUID edgeId = edge.getKey().getEdgeEntityKeyId();

        UUID newSrcId = getMergedId( linkedEntitySetId, srcId );
        if ( newSrcId != null ) {
            srcEntitySetId = linkedEntitySetId;
            srcSyncId = syncId;
            srcId = newSrcId;
        }
        UUID newDstId = getMergedId( linkedEntitySetId, dstId );
        if ( newDstId != null ) {
            dstEntitySetId = linkedEntitySetId;
            dstSyncId = syncId;
            dstId = newDstId;
        }
        UUID newEdgeId = getMergedId( linkedEntitySetId, edgeId );
        if ( newEdgeId != null ) {
            edgeEntitySetId = linkedEntitySetId;
            edgeId = newEdgeId;
        }

        graph.addEdge( srcId,
                edge.getSrcTypeId(),
                srcEntitySetId,
                srcSyncId,
                dstId,
                edge.getDstTypeId(),
                dstEntitySetId,
                dstSyncId,
                edgeId,
                edge.getEdgeTypeId(),
                edgeEntitySetId );
    }
}
