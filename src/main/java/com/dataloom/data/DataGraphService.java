package com.dataloom.data;

import static com.google.common.util.concurrent.Futures.transformAsync;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.analysis.requests.TopUtilizerDetails;
import com.dataloom.data.requests.Association;
import com.dataloom.data.requests.Entity;
import com.dataloom.data.storage.CassandraEntityDatastore;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.graph.core.LoomGraph;
import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mappers.ObjectMappers;
import com.datastax.driver.core.ResultSetFuture;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.exceptions.ResourceNotFoundException;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class DataGraphService implements DataGraphManager {
    private static final Logger            logger = LoggerFactory
            .getLogger( DataGraphService.class );
    private final ListeningExecutorService executor;
    private EventBus                       eventBus;
    private LoomGraph                      lm;
    private EntityKeyIdService             idService;
    private EntityDatastore                eds;
    // Get entity type id by entity set id, cached.
    // TODO HC: Local caching is needed because this would be called very often, so direct calls to IMap should be
    // minimized. Nonetheless, this certainly should be refactored into EdmService or something.
    private IMap<UUID, EntitySet>          entitySets;
    private LoadingCache<UUID, UUID>       typeIds;

    public DataGraphService(
            HazelcastInstance hazelcastInstance,
            CassandraEntityDatastore eds,
            LoomGraph lm,
            EntityKeyIdService ids,
            ListeningExecutorService executor,
            EventBus eventBus ) {
        this.lm = lm;
        this.idService = ids;
        this.eds = eds;
        this.executor = executor;
        this.eventBus = eventBus;

        this.entitySets = hazelcastInstance.getMap( HazelcastMap.ENTITY_SETS.name() );
        this.typeIds = CacheBuilder.newBuilder()
                .maximumSize( 100000 ) // 100K * 16 = 16000K = 16MB
                .build( new CacheLoader<UUID, UUID>() {

                    @Override
                    public UUID load( UUID key ) throws Exception {
                        return entitySets.get( key ).getEntityTypeId();
                    }
                } );
    }

    public static void tryGetAndLogErrors( ListenableFuture<?> f ) {
        try {
            f.get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Future execution failed.", e );
        }
    }

    @Override
    public EntitySetData getEntitySetData(
            UUID entitySetId,
            UUID syncId,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        return eds.getEntitySetData( entitySetId, syncId, authorizedPropertyTypes );
    }

    @Override
    public EntitySetData getLinkedEntitySetData(
            UUID linkedEntitySetId,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesForEntitySets ) {
        return eds.getLinkedEntitySetData( linkedEntitySetId, authorizedPropertyTypesForEntitySets );
    }

    @Override
    public void deleteEntitySetData( UUID entitySetId ) {
        eds.deleteEntitySetData( entitySetId );
        // TODO delete all vertices
    }

    @Override
    public void updateEntity(
            UUID id,
            SetMultimap<UUID, Object> entityDetails,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType ) {
        EntityKey elementReference = idService.getEntityKey( id );
        updateEntity( elementReference, entityDetails, authorizedPropertiesWithDataType );
    }

    @Override
    public void updateEntity(
            EntityKey entityKey,
            SetMultimap<UUID, Object> entityDetails,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType ) {
        eds.updateEntity( entityKey, entityDetails, authorizedPropertiesWithDataType );
    }

    @Override
    public void deleteEntity( UUID elementId ) {
        EntityKey entityKey = idService.getEntityKey( elementId );
        lm.deleteVertex( elementId );
        eds.deleteEntity( entityKey );
    }

    @Override
    public void deleteAssociation( EdgeKey key ) {
        EntityKey entityKey = idService.getEntityKey( key.getEdgeEntityKeyId() );
        lm.deleteEdge( key );
        eds.deleteEntity( entityKey );
    }

    @Override
    public void createEntities(
            UUID entitySetId,
            UUID syncId,
            Map<String, SetMultimap<UUID, Object>> entities,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType )
            throws ExecutionException, InterruptedException {
        entities.entrySet()
                .parallelStream()
                .flatMap(
                        entity -> {
                            final EntityKey key = new EntityKey( entitySetId, entity.getKey(), syncId );
                            return createEntity( key, entity.getValue(), authorizedPropertiesWithDataType );
                        } )
                .forEach( DataGraphService::tryGetAndLogErrors );
    }

    private Stream<ListenableFuture> createEntity(
            EntityKey key,
            SetMultimap<UUID, Object> details,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType ) {
        final ListenableFuture reservationAndVertex = transformAsync( idService.getEntityKeyIdAsync( key ),
                lm::createVertexAsync,
                executor );
        final ListenableFuture writes = eds.updateEntityAsync( key, details, authorizedPropertiesWithDataType );
        return Stream.of( reservationAndVertex, writes );
    }

    @Override
    public void createAssociations(
            UUID entitySetId,
            UUID syncId,
            Set<Association> associations,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType )
            throws InterruptedException, ExecutionException {
        List<ListenableFuture> futures = new ArrayList<ListenableFuture>( 2 * associations.size() );

        associations
                .parallelStream()
                .flatMap( association -> {
                    UUID edgeId = idService.getEntityKeyId( association.getKey() );

                    ListenableFuture writes = Futures.allAsList( eds.updateEntityAsync( association.getKey(),
                            association.getDetails(),
                            authorizedPropertiesWithDataType ) );

                    UUID srcId = idService.getEntityKeyId( association.getSrc() );
                    UUID srcTypeId = typeIds.getUnchecked( association.getSrc().getEntitySetId() );
                    UUID dstId = idService.getEntityKeyId( association.getDst() );
                    UUID dstTypeId = typeIds.getUnchecked( association.getDst().getEntitySetId() );
                    UUID edgeTypeId = typeIds.getUnchecked( association.getKey().getEntitySetId() );

                    ListenableFuture addEdge = Futures.allAsList( lm
                            .addEdgeAsync( srcId, srcTypeId, dstId, dstTypeId, edgeId, edgeTypeId ) );
                    return Stream.of( writes, addEdge );
                } ).forEach( DataGraphService::tryGetAndLogErrors );
    }

    @Override
    public void createEntitiesAndAssociations(
            Set<Entity> entities,
            Set<Association> associations,
            Map<UUID, Map<UUID, EdmPrimitiveTypeKind>> authorizedPropertiesByEntitySetId )
            throws InterruptedException, ExecutionException {
        Map<EntityKey, UUID> idsRegistered = new HashMap<>();

        entities.parallelStream()
                .flatMap( entity -> createEntity( entity.getKey(),
                        entity.getDetails(),
                        authorizedPropertiesByEntitySetId.get( entity.getKey().getEntitySetId() ) ) )
                .forEach( DataGraphService::tryGetAndLogErrors );

        associations.parallelStream().flatMap( association -> {
            UUID srcId = idService.getEntityKeyId( association.getSrc() );
            UUID dstId = idService.getEntityKeyId( association.getDst() );
            if ( srcId == null || dstId == null ) {
                String err = String.format(
                        "Edge %s cannot be created because some vertices failed to register for an id.",
                        association.toString() );
                logger.debug( err );
                return Stream.of( Futures.immediateFailedFuture( new ResourceNotFoundException( err ) ) );
            } else {
                ListenableFuture writes = Futures.allAsList( eds.updateEntityAsync( association.getKey(),
                        association.getDetails(),
                        authorizedPropertiesByEntitySetId.get( association.getKey().getEntitySetId() ) ) );

                UUID srcTypeId = typeIds.getUnchecked( association.getSrc().getEntitySetId() );
                UUID dstTypeId = typeIds.getUnchecked( association.getDst().getEntitySetId() );
                UUID edgeId = idService.getEntityKeyId( association.getKey() );
                UUID edgeTypeId = typeIds.getUnchecked( association.getKey().getEntitySetId() );

                ListenableFuture addEdge = Futures
                        .allAsList( lm.addEdgeAsync( srcId, srcTypeId, dstId, dstTypeId, edgeId, edgeTypeId ) );
                return Stream.of( writes, addEdge );
            }
        } ).forEach( DataGraphService::tryGetAndLogErrors );
    }

    @Override
    public EntitySetData getTopUtilizers(
            UUID entitySetId,
            UUID syncId,
            List<TopUtilizerDetails> topUtilizerDetailsList,
            int numResults,
            Map<UUID, PropertyType> authorizedPropertyTypes )
            throws InterruptedException, ExecutionException {
        ByteBuffer queryId;
        try {
            queryId = ByteBuffer.wrap( ObjectMappers.getSmileMapper().writeValueAsBytes( topUtilizerDetailsList ) );
        } catch ( JsonProcessingException e1 ) {
            logger.debug( "Unable to generate query id." );
            return null;
        }
        if ( !eds.queryAlreadyExecuted( queryId ) ) {
            eds.getEntityKeysForEntitySet( entitySetId, syncId ).parallel().map( entityKey -> {
                UUID vertexId = idService.getEntityKeyId( entityKey );
                List<ResultSetFuture> countFutures = new ArrayList<>();
                for ( TopUtilizerDetails details : topUtilizerDetailsList ) {
                    countFutures.add( lm.getEdgeCount( vertexId,
                            details.getAssociationTypeId(),
                            details.getNeighborTypeIds(),
                            details.getUtilizerIsSrc() ) );
                }
    
                int score = 0;
                for ( ResultSetFuture f : countFutures ) {
                    try {
                        score += f.get().one().getLong( 0 );
                    } catch ( InterruptedException | ExecutionException e ) {
                        logger.debug( "Unable to count edges for vertex id." );
                    }
                }
                eds.writeVertexCount( queryId, vertexId, score * 1.0 );
                return score;
            } ).collect( Collectors.toList() );
        }

        Iterable<SetMultimap<FullQualifiedName, Object>> entities = Iterables
                .transform( eds.readTopUtilizers( queryId, numResults ), vertexId -> {
                    EntityKey key = idService.getEntityKey( vertexId );
                    return eds.getEntity( key.getEntitySetId(),
                            key.getSyncId(),
                            key.getEntityId(),
                            authorizedPropertyTypes );
                } );

        Set<FullQualifiedName> properties = authorizedPropertyTypes.values().stream()
                .map( propertyType -> propertyType.getType() ).collect( Collectors.toSet() );

        return new EntitySetData( properties, entities );
    }
}
