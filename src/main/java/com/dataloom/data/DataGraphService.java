package com.dataloom.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.data.requests.Association;
import com.dataloom.data.requests.Entity;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.graph.core.LoomGraph;
import com.dataloom.graph.core.objects.EdgeKey;
import com.dataloom.graph.core.objects.LoomVertex;
import com.dataloom.graph.core.objects.LoomVertexFuture;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.eventbus.EventBus;
import com.kryptnostic.datastore.services.CassandraDataManager;

public class DataGraphService implements DataGraphManager {
    private static final Logger  logger     = LoggerFactory
            .getLogger( DataGraphService.class );

    @Inject
    private EventBus             eventBus;

    private CassandraDataManager cdm;
    private LoomGraph            lm;

    private static final int     bufferSize = 1000;

    public DataGraphService(
            CassandraDataManager cdm,
            LoomGraph lm ) {
        this.cdm = cdm;
        this.lm = lm;
    }

    @Override
    public Iterable<SetMultimap<FullQualifiedName, Object>> getEntitySetData(
            UUID entitySetId,
            UUID syncId,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        return cdm.getEntitySetData( entitySetId, syncId, authorizedPropertyTypes );
    }

    @Override
    public Iterable<SetMultimap<FullQualifiedName, Object>> getLinkedEntitySetData(
            UUID linkedEntitySetId,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesForEntitySets ) {
        return cdm.getLinkedEntitySetData( linkedEntitySetId, authorizedPropertyTypesForEntitySets );
    }
    
    @Override
    public void deleteEntitySetData( UUID entitySetId ) {
        cdm.deleteEntitySetData( entitySetId );
        //TODO delete all vertices
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
        cdm.createData( vertexReference.getEntitySetId(), vertexReference.getSyncId(), authorizedPropertiesWithDataType, authorizedPropertiesWithDataType.keySet(), vertexReference.getEntityId(), entityDetails );
    }

    @Override
    public void updateAssociation(
            EdgeKey key,
            SetMultimap<UUID, Object> entityDetails,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType ) {
        //Remark: current createData is really upsertData, given how Cassandra handles inserts/updates
        cdm.createData( key.getReference().getEntitySetId(), key.getReference().getSyncId(), authorizedPropertiesWithDataType, authorizedPropertiesWithDataType.keySet(), key.getReference().getEntityId(), entityDetails );
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
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType ) {
        Set<UUID> authorizedProperties = authorizedPropertiesWithDataType.keySet();

        List<ResultSetFuture> datafs = new ArrayList<ResultSetFuture>();
        List<LoomVertexFuture> vertexfs = new ArrayList<LoomVertexFuture>();

        entities.entrySet().stream().forEach( entity -> {
            cdm.createDataAsync( entitySetId,
                    syncId,
                    authorizedPropertiesWithDataType,
                    authorizedProperties,
                    datafs,
                    entity.getKey(),
                    entity.getValue() );
            if ( datafs.size() > bufferSize ) {
                datafs.forEach( ResultSetFuture::getUninterruptibly );
                datafs = new ArrayList<ResultSetFuture>();
            }

            vertexfs.add( lm.createVertexAsync( new EntityKey( entitySetId, entity.getKey(), syncId ) ) );
            if ( vertexfs.size() > bufferSize ) {
                vertexfs.forEach( LoomVertexFuture::getUninterruptibly );
                vertexfs = new ArrayList<LoomVertexFuture>();
            }
        } );

        datafs.forEach( ResultSetFuture::getUninterruptibly );
        vertexfs.forEach( LoomVertexFuture::getUninterruptibly );
    }

    @Override
    public void createAssociations(
            UUID entitySetId,
            UUID syncId,
            Set<Association> associations,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType ) {
        Set<UUID> authorizedProperties = authorizedPropertiesWithDataType.keySet();

        List<ResultSetFuture> datafs = new ArrayList<ResultSetFuture>();

        associations.stream().forEach( association -> {
            cdm.createDataAsync( entitySetId,
                    syncId,
                    authorizedPropertiesWithDataType,
                    authorizedProperties,
                    datafs,
                    association.getKey().getEntityId(),
                    association.getDetails() );

            LoomVertex src = lm.getVertexByEntityKey( association.getSrc() );
            LoomVertex dst = lm.getVertexByEntityKey( association.getDst() );
            if ( src == null || dst == null ) {
                logger.error( "Edge for entity id {} cannot be created because one of its vertices was not created.",
                        association.getKey().getEntityId() );
            }

            datafs.add( lm.addEdgeAsync( src, dst, association.getKey() ) );

            if ( datafs.size() > bufferSize ) {
                datafs.forEach( ResultSetFuture::getUninterruptibly );
                datafs = new ArrayList<ResultSetFuture>();
            }

        } );
        datafs.forEach( ResultSetFuture::getUninterruptibly );
    }

    @Override
    public void createEntitiesAndAssociations(
            Iterable<Entity> entities,
            Iterable<Association> associations,
            Map<UUID, Map<UUID, EdmPrimitiveTypeKind>> authorizedPropertiesByEntitySetId ) {
        Map<EntityKey, LoomVertexFuture> vertexfs = Maps.newHashMap();
        List<ResultSetFuture> datafs = new ArrayList<ResultSetFuture>();

        entities.forEach( entity -> {
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

            vertexfs.put( entity.getKey(), lm.createVertexAsync( entity.getKey() ) );
        } );
        Map<EntityKey, LoomVertex> verticesCreated = Maps.transformValues( vertexfs,
                LoomVertexFuture::getUninteruptibly );

        associations.forEach( association -> {
            LoomVertex src = verticesCreated.get( association.getSrc() );
            LoomVertex dst = verticesCreated.get( association.getDst() );
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
        } );

        datafs.forEach( ResultSetFuture::getUninterruptibly );
    }

}
