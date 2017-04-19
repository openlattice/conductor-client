package com.dataloom.data;

import com.dataloom.data.requests.Association;
import com.dataloom.data.requests.Entity;
import com.dataloom.data.storage.CassandraEntityDatastore;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.graph.core.LoomGraph;
import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.SetMultimap;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.google.common.util.concurrent.Futures.transformAsync;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class DataGraphService implements DataGraphManager {
    private static final Logger logger = LoggerFactory
            .getLogger( DataGraphService.class );
    private final ListeningExecutorService executor;
    private       EventBus                 eventBus;
    private       LoomGraph                lm;
    private       EntityKeyIdService       idService;
    private       EntityDatastore          eds;
    // Get entity type id by entity set id, cached.
    // TODO HC: Local caching is needed because this would be called very often, so direct calls to IMap should be
    // minimized. Nonetheless, this certainly should be refactored into EdmService or something.
    private       IMap<UUID, EntitySet>    entitySets;
    private       LoadingCache<UUID, UUID> typeIds;

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
                .maximumSize( 100000 ) //100K * 16 = 16000K = 16MB
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
                            final ListenableFuture reservation = idService.getEntityKeyIdAsync( key );
                            final ListenableFuture writes = eds.updateEntityAsync( key,
                                    entity.getValue(),
                                    authorizedPropertiesWithDataType );
                            return Stream.of( reservation, writes );
                        } )
                .forEach( DataGraphService::tryGetAndLogErrors );
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

                    ListenableFuture addEdge = lm
                            .addEdgeAsync( srcId, srcTypeId, dstId, dstTypeId, edgeId, edgeTypeId );
                    return Stream.of( writes, addEdge );
                } ).forEach( DataGraphService::tryGetAndLogErrors );
    }

    @Override
    public void createEntitiesAndAssociations(
            Iterable<Entity> entities,
            Iterable<Association> associations,
            Map<UUID, Map<UUID, EdmPrimitiveTypeKind>> authorizedPropertiesByEntitySetId )
            throws InterruptedException, ExecutionException {
        Map<EntityKey, UUID> idsRegistered = new HashMap<>();
        List<ListenableFuture> entityFutures = new ArrayList<>();
        List<ListenableFuture> dataFutures = new ArrayList<>();

        for ( Entity entity : entities ) {

            entityFutures.add( transformAsync( idService.getOrCreateAsync( entity.getKey() ),
                    id -> {
                        idsRegistered.put( entity.getKey(), id );
                        return lm.createVertexAsync( id, entity.getKey() );
                    } ) );

            dataFutures.addAll( eds.updateEntityAsync( entity.getKey(),
                    entity.getDetails(),
                    authorizedPropertiesByEntitySetId.get( entity.getKey().getEntitySetId() ) ) );
        }

        for ( ListenableFuture f : entityFutures ) {
            f.get();
        }

        for ( Association association : associations ) {
            UUID srcId = idsRegistered.get( association.getSrc() );
            UUID dstId = idsRegistered.get( association.getDst() );
            if ( srcId == null || dstId == null ) {
                logger.debug( "Edge with id {} cannot be created because some vertices failed to register for an id.",
                        association.getKey().getEntityId() );
            } else {
                dataFutures.add( Futures.successfulAsList( eds.updateEntityAsync( association.getKey(),
                        association.getDetails(),
                        authorizedPropertiesByEntitySetId.get( association.getKey().getEntitySetId() ) ) ) );

                UUID srcTypeId = typeIds.getUnchecked( association.getSrc().getEntitySetId() );
                UUID dstTypeId = typeIds.getUnchecked( association.getDst().getEntitySetId() );
                UUID edgeId = idService.getOrCreate( association.getKey() );
                UUID edgeTypeId = typeIds.getUnchecked( association.getKey().getEntitySetId() );

                dataFutures.add( lm.addEdgeAsync( srcId, srcTypeId, dstId, dstTypeId, edgeId, edgeTypeId ) );
            }
        }
        for ( ListenableFuture f : dataFutures ) {
            f.get();
        }
    }
}
