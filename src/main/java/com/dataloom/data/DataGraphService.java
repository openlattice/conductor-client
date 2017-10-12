package com.dataloom.data;

import static com.google.common.util.concurrent.Futures.transformAsync;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.analysis.requests.TopUtilizerDetails;
import com.dataloom.data.analytics.IncrementableWeightId;
import com.dataloom.data.requests.Association;
import com.dataloom.data.requests.Entity;
import com.dataloom.data.storage.CassandraEntityDatastore;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.graph.core.LoomGraph;
import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.exceptions.ResourceNotFoundException;
import org.apache.commons.collections4.keyvalue.MultiKey;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class DataGraphService implements DataGraphManager {
    private static final Logger logger = LoggerFactory
            .getLogger( DataGraphService.class );
    private final ListeningExecutorService executor;
    private final Cache<MultiKey, IncrementableWeightId[]> queryCache = CacheBuilder.newBuilder()
            .maximumSize( 1000 )
            .expireAfterWrite( 30, TimeUnit.SECONDS )
            .build();
    private EventBus                 eventBus;
    private LoomGraph                lm;
    private EntityKeyIdService       idService;
    private EntityDatastore          eds;
    // Get entity type id by entity set id, cached.
    // TODO HC: Local caching is needed because this would be called very often, so direct calls to IMap should be
    // minimized. Nonetheless, this certainly should be refactored into EdmService or something.
    private IMap<UUID, EntitySet>    entitySets;
    private LoadingCache<UUID, UUID> typeIds;

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

    @Override
    public EntitySetData<FullQualifiedName> getEntitySetData(
            UUID entitySetId,
            UUID syncId,
            LinkedHashSet<String> orderedPropertyNames,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        return eds.getEntitySetData( entitySetId, syncId, orderedPropertyNames, authorizedPropertyTypes );
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
    public UUID createEntity(
            UUID entitySetId,
            UUID syncId,
            String entityId,
            SetMultimap<UUID, Object> entityDetails,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType )
            throws ExecutionException, InterruptedException {

        final EntityKey key = new EntityKey( entitySetId, entityId, syncId );
        createEntity( key, entityDetails, authorizedPropertiesWithDataType )
                .forEach( DataGraphService::tryGetAndLogErrors );
        return idService.getEntityKeyId( key );
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
        final ListenableFuture reservationAndVertex = idService.getEntityKeyIdAsync( key );
        final Stream<ListenableFuture> writes = eds.updateEntityAsync( key, details, authorizedPropertiesWithDataType );
        return Stream.concat( Stream.of( reservationAndVertex ), writes );
    }

    @Override
    public void createAssociations(
            UUID entitySetId,
            UUID syncId,
            Set<Association> associations,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType )
            throws InterruptedException, ExecutionException {
        // List<ListenableFuture> futures = new ArrayList<ListenableFuture>( 2 * associations.size() );

        associations
                .parallelStream()
                .flatMap( association -> {
                    UUID edgeId = idService.getEntityKeyId( association.getKey() );

                    Stream<ListenableFuture> writes = eds.updateEntityAsync( association.getKey(),
                            association.getDetails(),
                            authorizedPropertiesWithDataType );

                    UUID srcId = idService.getEntityKeyId( association.getSrc() );
                    UUID srcTypeId = typeIds.getUnchecked( association.getSrc().getEntitySetId() );
                    UUID srcSetId = association.getSrc().getEntitySetId();
                    UUID srcSyncId = association.getSrc().getSyncId();
                    UUID dstId = idService.getEntityKeyId( association.getDst() );
                    UUID dstTypeId = typeIds.getUnchecked( association.getDst().getEntitySetId() );
                    UUID dstSetId = association.getDst().getEntitySetId();
                    UUID dstSyncId = association.getDst().getSyncId();
                    UUID edgeTypeId = typeIds.getUnchecked( association.getKey().getEntitySetId() );
                    UUID edgeSetId = association.getKey().getEntitySetId();

                    ListenableFuture addEdge = lm
                            .addEdgeAsync( srcId,
                                    srcTypeId,
                                    srcSetId,
                                    srcSyncId,
                                    dstId,
                                    dstTypeId,
                                    dstSetId,
                                    dstSyncId,
                                    edgeId,
                                    edgeTypeId,
                                    edgeSetId );
                    return Stream.concat( writes, Stream.of( addEdge ) );
                } ).forEach( DataGraphService::tryGetAndLogErrors );
    }

    @Override
    public void createEntitiesAndAssociations(
            Set<Entity> entities,
            Set<Association> associations,
            Map<UUID, Map<UUID, EdmPrimitiveTypeKind>> authorizedPropertiesByEntitySetId )
            throws InterruptedException, ExecutionException {
        // Map<EntityKey, UUID> idsRegistered = new HashMap<>();

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
                Stream<ListenableFuture> writes = eds.updateEntityAsync( association.getKey(),
                        association.getDetails(),
                        authorizedPropertiesByEntitySetId.get( association.getKey().getEntitySetId() ) );

                UUID srcTypeId = typeIds.getUnchecked( association.getSrc().getEntitySetId() );
                UUID srcSetId = association.getSrc().getEntitySetId();
                UUID srcSyncId = association.getSrc().getSyncId();
                UUID dstTypeId = typeIds.getUnchecked( association.getDst().getEntitySetId() );
                UUID dstSetId = association.getDst().getEntitySetId();
                UUID dstSyncId = association.getDst().getSyncId();
                UUID edgeId = idService.getEntityKeyId( association.getKey() );
                UUID edgeTypeId = typeIds.getUnchecked( association.getKey().getEntitySetId() );
                UUID edgeSetId = association.getKey().getEntitySetId();

                ListenableFuture addEdge = lm.addEdgeAsync( srcId,
                        srcTypeId,
                        srcSetId,
                        srcSyncId,
                        dstId,
                        dstTypeId,
                        dstSetId,
                        dstSyncId,
                        edgeId,
                        edgeTypeId,
                        edgeSetId );
                return Stream.concat( writes, Stream.of( addEdge ) );
            }
        } ).forEach( DataGraphService::tryGetAndLogErrors );
    }

    @Override
    public Iterable<SetMultimap<Object, Object>> getTopUtilizers(
            UUID entitySetId,
            UUID syncId,
            List<TopUtilizerDetails> topUtilizerDetailsList,
            int numResults,
            Map<UUID, PropertyType> authorizedPropertyTypes )
            throws InterruptedException, ExecutionException {
        /*
         * ByteBuffer queryId; try { queryId = ByteBuffer.wrap( ObjectMappers.getSmileMapper().writeValueAsBytes(
         * topUtilizerDetailsList ) ); } catch ( JsonProcessingException e1 ) { logger.debug(
         * "Unable to generate query id." ); return null; }
         */
        IncrementableWeightId[] maybeUtilizers = queryCache
                .getIfPresent( new MultiKey( entitySetId, topUtilizerDetailsList ) );
        final IncrementableWeightId[] utilizers;
        // if ( !eds.queryAlreadyExecuted( queryId ) ) {
        if ( maybeUtilizers == null ) {
            //            utilizers = new TopUtilizers( numResults );
            SetMultimap<UUID, UUID> srcFilters = HashMultimap.create();
            SetMultimap<UUID, UUID> dstFilters = HashMultimap.create();

            topUtilizerDetailsList.forEach( details -> {
                ( details.getUtilizerIsSrc() ? srcFilters : dstFilters ).
                        putAll( details.getAssociationTypeId(), details.getNeighborTypeIds() );

            } );
            utilizers = lm.computeGraphAggregation( numResults, entitySetId, syncId, srcFilters, dstFilters );
            //            eds.getEntityKeysForEntitySet( entitySetId, syncId )
            //                    .parallel()
            //                    .map( idService::getEntityKeyId )
            //                    .forEach( vertexId -> {
            //                        long score = topUtilizerDetailsList.parallelStream()
            //                                /*.map( details -> lm.getEdgeCount( vertexId,
            //                                        details.getAssociationTypeId(),
            //                                        details.getNeighborTypeIds(),
            //                                        details.getUtilizerIsSrc() ) )
            //                                .map( ResultSetFuture::getUninterruptibly )*/
            //                                .mapToLong( details -> lm.getHazelcastEdgeCount( vertexId,
            //                                        details.getAssociationTypeId(),
            //                                        details.getNeighborTypeIds(),
            //                                        details.getUtilizerIsSrc() ) )
            //                                //.mapToLong( Util::getCount )
            //                                .sum();
            //                        utilizers.accumulate( vertexId, score );
            //                        // eds.writeVertexCount( queryId, vertexId, 1.0D * score );
            //                    } );

            queryCache.put( new MultiKey( entitySetId, topUtilizerDetailsList ), utilizers );
        } else {
            utilizers = maybeUtilizers;
        }

        return eds.getEntities( utilizers, authorizedPropertyTypes )::iterator;
        //
        //        return utilizers
        //                .stream()
        //                .map( longWeightedId -> {
        //                    UUID vertexId = longWeightedId.getId();
        //                    EntityKey key = idService.getEntityKey( vertexId );
        //                    SetMultimap<Object, Object> entity = HashMultimap.create();
        //                    entity.put( "count", longWeightedId.getWeight() );
        //                    entity.putAll(
        //                            eds.getEntity( key.getEntitySetId(),
        //                                    key.getSyncId(),
        //                                    key.getEntityId(),
        //                                    authorizedPropertyTypes ) );
        //                    entity.put( "id", vertexId.toString() );
        //                    return entity;
        //                } )::iterator;

        /*
         * Iterable<SetMultimap<Object, Object>> entities = Iterables .transform( eds.readTopUtilizers( queryId,
         * numResults ), vertexId -> { EntityKey key = idService.getEntityKey( vertexId ); SetMultimap<Object, Object>
         * entity = HashMultimap.create(); entity.putAll( eds.getEntity( key.getEntitySetId(), key.getSyncId(),
         * key.getEntityId(), authorizedPropertyTypes ) ); entity.put( "id", vertexId.toString() ); return entity; } );
         * return entities;
         */
    }

    public static void tryGetAndLogErrors( ListenableFuture<?> f ) {
        try {
            f.get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Future execution failed.", e );
        }
    }
}
