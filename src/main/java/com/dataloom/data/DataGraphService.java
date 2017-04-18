package com.dataloom.data;

import com.dataloom.data.requests.Association;
import com.dataloom.data.requests.Entity;
import com.dataloom.data.storage.CassandraEntityDatastore;
import com.dataloom.graph.core.LoomGraph;
import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.graph.core.objects.LoomVertexKey;
import com.dataloom.graph.core.objects.LoomVertexFuture;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.google.common.util.concurrent.Futures.transformAsync;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class DataGraphService implements DataGraphManager {
    private static final Logger logger     = LoggerFactory
            .getLogger( DataGraphService.class );
    private static final int    bufferSize = 1000;
    @Inject
    private EventBus                 eventBus;
    private CassandraEntityDatastore cdm;
    private LoomGraph                lm;
    private EntityKeyIdService       idService;
    private EntityDatastore          eds;

    public DataGraphService(
            CassandraEntityDatastore cdm,
            LoomGraph lm,
            EntityKeyIdService ids ) {
        this.cdm = cdm;
        this.lm = lm;
        this.idService = ids;
        this.eds = cdm;
    }

    @Override
    public void updateEntity(
            UUID vertexId,
            SetMultimap<UUID, Object> entityDetails,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType ) {
        EntityKey vertexReference = lm.getVertexById( vertexId ).getReference();
        updateEntity( vertexReference, entityDetails, authorizedPropertiesWithDataType );
    }

    @Override
    public void updateEntity(
            EntityKey vertexReference,
            SetMultimap<UUID, Object> entityDetails,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType ) {
        cdm.createData( vertexReference.getEntitySetId(),
                vertexReference.getSyncId(),
                authorizedPropertiesWithDataType,
                authorizedPropertiesWithDataType.keySet(),
                vertexReference.getEntityId(),
                entityDetails );
    }

    @Override
    public void updateAssociation(
            EdgeKey key,
            SetMultimap<UUID, Object> entityDetails,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType ) {
        // Remark: current createData is really upsertData, given how Cassandra handles inserts/updates
        cdm.createData( key.getReference().getEntitySetId(),
                key.getReference().getSyncId(),
                authorizedPropertiesWithDataType,
                authorizedPropertiesWithDataType.keySet(),
                key.getReference().getEntityId(),
                entityDetails );
    }

    @Override
    public void deleteEntity( UUID vertexId ) {
        EntityKey entityKey = lm.getVertexById( vertexId ).getReference();
        lm.deleteVertex( vertexId );
        cdm.deleteEntity( entityKey );
    }

    @Override
    public void deleteAssociation( EdgeKey key ) {
        lm.deleteEdge( key );
        cdm.deleteEntity( key.getReference() );
    }

    @Override
    public void createEntities(
            UUID entitySetId,
            UUID syncId,
            Map<String, SetMultimap<UUID, Object>> entities,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType )
            throws ExecutionException, InterruptedException {
        List<ListenableFuture> futures = new ArrayList<>( 2 * entities.size() );

        for ( Map.Entry<String, SetMultimap<UUID, Object>> entity : entities.entrySet() ) {
            final String entityId = entity.getKey();
            EntityKey key = new EntityKey( entitySetId, entityId, syncId );
            futures.add( transformAsync( idService.getOrCreateAsync( key ),
                    id -> lm.createVertexAsync( id, key ) ) );
            futures.add( eds.updateEntityAsync( key, entity.getValue(), authorizedPropertiesWithDataType ) );
        }

        for( ListenableFuture f : futures ) {
            f.get();
        }
    }

    @Override
    public void createAssociations(
            UUID entitySetId,
            UUID syncId,
            Set<Association> associations,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType ) {
        Set<UUID> authorizedProperties = authorizedPropertiesWithDataType.keySet();

        List<ResultSetFuture> datafs = new ArrayList<ResultSetFuture>();

        for ( Association association : associations ) {
            cdm.createDataAsync( entitySetId,
                    syncId,
                    authorizedPropertiesWithDataType,
                    authorizedProperties,
                    datafs,
                    association.getKey().getEntityId(),
                    association.getDetails() );

            LoomVertexKey src = lm.getVertexByEntityKey( association.getSrc() );
            LoomVertexKey dst = lm.getVertexByEntityKey( association.getDst() );

            datafs.add( lm.addEdgeAsync( src, dst, association.getKey() ) );

            if ( datafs.size() > bufferSize ) {
                datafs.forEach( ResultSetFuture::getUninterruptibly );
                datafs = new ArrayList<ResultSetFuture>();
            }
        }
        datafs.forEach( ResultSetFuture::getUninterruptibly );
    }

    @Override
    public void createEntitiesAndAssociations(
            Iterable<Entity> entities,
            Iterable<Association> associations,
            Map<UUID, Map<UUID, EdmPrimitiveTypeKind>> authorizedPropertiesByEntitySetId ) {
        Map<EntityKey, LoomVertexFuture> vertexfs = Maps.newHashMap();
        List<ResultSetFuture> datafs = new ArrayList<>();

        for ( Entity entity : entities ) {
            cdm.createDataAsync( entity.getKey().getEntitySetId(),
                    entity.getKey().getSyncId(),
                    authorizedPropertiesByEntitySetId.get( entity.getKey().getEntitySetId() ),
                    authorizedPropertiesByEntitySetId.get( entity.getKey().getEntitySetId() ).keySet(),
                    datafs,
                    entity.getKey().getEntityId(),
                    entity.getDetails() );

            if ( datafs.size() > bufferSize ) {
                datafs.forEach( ResultSetFuture::getUninterruptibly );
                datafs = new ArrayList<ResultSetFuture>();
            }

            vertexfs.put( entity.getKey(), lm.getOrCreateVertexAsync( entity.getKey() ) );
        }

        Map<EntityKey, LoomVertexKey> verticesCreated = Maps.transformValues( vertexfs,
                LoomVertexFuture::get );

        for ( Association association : associations ) {
            LoomVertexKey src = verticesCreated.get( association.getSrc() );
            LoomVertexKey dst = verticesCreated.get( association.getDst() );
            if ( src == null || dst == null ) {
                logger.debug( "Edge with id {} cannot be created because one of its vertices was not created.",
                        association.getKey().getEntityId() );
            } else {
                cdm.createDataAsync( association.getKey().getEntitySetId(),
                        association.getKey().getSyncId(),
                        authorizedPropertiesByEntitySetId.get( association.getKey().getEntitySetId() ),
                        authorizedPropertiesByEntitySetId.get( association.getKey().getEntitySetId() ).keySet(),
                        datafs,
                        association.getKey().getEntityId(),
                        association.getDetails() );

                datafs.add( lm.addEdgeAsync( src, dst, association.getKey() ) );

                if ( datafs.size() > bufferSize ) {
                    datafs.forEach( ResultSetFuture::getUninterruptibly );
                    datafs = new ArrayList<ResultSetFuture>();
                }
            }
        }
        datafs.forEach( ResultSetFuture::getUninterruptibly );
    }

}
