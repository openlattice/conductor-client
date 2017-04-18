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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import com.dataloom.data.DatasourceManager;
import com.dataloom.data.EntityDatastore;
import com.dataloom.data.EntityKey;
import com.dataloom.data.EntitySetData;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.data.events.EntityDataCreatedEvent;
import com.dataloom.data.events.EntityDataDeletedEvent;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.graph.core.LoomGraph;
import com.dataloom.linking.HazelcastLinkingGraphs;
import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CassandraSerDesFactory;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class CassandraEntityDatastore implements EntityDatastore {

    @Inject
    private EventBus                     eventBus;

    private static final Logger          logger = LoggerFactory
            .getLogger( CassandraEntityDatastore.class );

    private final Session                session;
    private final ObjectMapper           mapper;
    private final HazelcastLinkingGraphs linkingGraph;
    private final DatasourceManager      dsm;

    private final PreparedStatement      writeDataQuery;

    private final PreparedStatement      entitySetQuery;
    private final PreparedStatement      entityIdsQuery;

    private final PreparedStatement      deleteEntityInAllSyncsQuery;
    private final PreparedStatement      deleteEntityQuery;

    private final PreparedStatement      linkedEntitiesQuery;

    private final PreparedStatement      readNumRPCRowsQuery;

    public CassandraEntityDatastore(
            Session session,
            ObjectMapper mapper,
            HazelcastLinkingGraphs linkingGraph,
            LoomGraph loomGraph,
            DatasourceManager dsm ) {
        this.session = session;
        this.mapper = mapper;
        this.linkingGraph = linkingGraph;
        this.dsm = dsm;

        CassandraTableBuilder dataTableDefinitions = Table.DATA.getBuilder();

        this.entitySetQuery = prepareEntitySetQuery( session, dataTableDefinitions );
        this.entityIdsQuery = prepareEntityIdsQuery( session );
        this.writeDataQuery = prepareWriteQuery( session, dataTableDefinitions );

        this.deleteEntityInAllSyncsQuery = prepareDeleteEntityInAllSyncsQuery( session );
        this.deleteEntityQuery = prepareDeleteEntityQuery( session );

        this.linkedEntitiesQuery = prepareLinkedEntitiesQuery( session );
        this.readNumRPCRowsQuery = prepareReadNumRPCRowsQuery( session );
    }

    @Override
    public EntitySetData getEntitySetData(
            UUID entitySetId,
            UUID syncId,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        Set<FullQualifiedName> authorizedPropertyFqns = authorizedPropertyTypes.values().stream()
                .map( pt -> pt.getType() ).collect( Collectors.toSet() );
        Iterable<ResultSet> entityRows = getRows( entitySetId, syncId, authorizedPropertyTypes.keySet() );
        return new EntitySetData( authorizedPropertyFqns, Iterables.transform( entityRows,
                rs -> rowToEntity( rs, authorizedPropertyTypes ) ) );
    }


    @Override
    public EntitySetData getLinkedEntitySetData(
            UUID linkedEntitySetId,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesForEntitySets ) {
        Set<FullQualifiedName> authorizedPropertyFqns = authorizedPropertyTypesForEntitySets.values().stream()
                .flatMap( map -> map.values().stream() )
                .map( pt -> pt.getType() ).collect( Collectors.toSet() );
        Iterable<Pair<UUID, Set<EntityKey>>> linkedEntityKeys = getLinkedEntityKeys( linkedEntitySetId );
        return new EntitySetData( authorizedPropertyFqns, Iterables.transform( linkedEntityKeys,
                linkedKey -> getAndMergeLinkedEntities( linkedEntitySetId,
                        linkedKey,
                        authorizedPropertyTypesForEntitySets ) )::iterator );
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
    public List<ResultSetFuture> updateEntityAsync(
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

    public Iterable<SetMultimap<UUID, Object>> getEntitySetDataIndexedById(
            UUID entitySetId,
            UUID syncId,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        Iterable<ResultSet> entityRows = getRows( entitySetId, syncId, authorizedPropertyTypes.keySet() );
        return Iterables.transform( entityRows,
                rs -> rowToEntityIndexedById( rs, authorizedPropertyTypes ) )::iterator;
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

    private Iterable<ResultSet> getRows(
            UUID entitySetId,
            UUID syncId,
            Set<UUID> authorizedProperties ) {
        Iterable<String> entityIds = getEntityIds( entitySetId );
        Iterable<ResultSetFuture> entityFutures;
        // If syncId is not specified, retrieve latest snapshot of entity
        final UUID finalSyncId;
        if ( syncId == null ) {
            finalSyncId = dsm.getCurrentSyncId( entitySetId );
        } else {
            finalSyncId = syncId;
        }
        entityFutures = Iterables.transform( entityIds,
                entityId -> asyncLoadEntity( entitySetId, entityId, finalSyncId, authorizedProperties ) );
        return Iterables.transform( entityFutures, ResultSetFuture::getUninterruptibly );
    }

    public Iterable<String> getEntityIds( UUID entitySetId ) {
        BoundStatement boundEntityIdsQuery = entityIdsQuery.bind()
                .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId );
        ResultSet entityIds = session.execute( boundEntityIdsQuery );
        return Iterables.filter( Iterables.transform( entityIds, RowAdapters::entityId ), StringUtils::isNotBlank );
    }

    public ResultSetFuture asyncLoadEntity(
            UUID entitySetId,
            String entityId,
            UUID syncId,
            Set<UUID> authorizedProperties ) {
        return session.executeAsync( entitySetQuery.bind()
                .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
                .setString( CommonColumns.ENTITYID.cql(), entityId )
                .setSet( CommonColumns.PROPERTY_TYPE_ID.cql(), authorizedProperties )
                .setUUID( CommonColumns.SYNCID.cql(), syncId ) );
    }

    /*
     * Warning: this loads ALL the properties of the entity, authorized or not.
     */
    public ResultSetFuture asyncLoadEntity(
            UUID entitySetId,
            String entityId,
            UUID syncId ) {
        return session.executeAsync( entitySetQuery.bind()
                .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
                .setString( CommonColumns.ENTITYID.cql(), entityId )
                .setUUID( CommonColumns.SYNCID.cql(), syncId ) );
    }

    @Deprecated
    public void createEntityData(
            UUID entitySetId,
            UUID syncId,
            Map<String, SetMultimap<UUID, Object>> entities,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType ) {
        Set<UUID> authorizedProperties = authorizedPropertiesWithDataType.keySet();

        List<ResultSetFuture> results = new ArrayList<ResultSetFuture>();

        entities.entrySet().stream().forEach( entity -> {
            results.addAll( createDataAsync( entitySetId,
                    syncId,
                    authorizedPropertiesWithDataType,
                    authorizedProperties,
                    entity.getKey(),
                    entity.getValue() ) );
        } );

        results.forEach( ResultSetFuture::getUninterruptibly );
    }

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
                entityDetails ).forEach( ResultSetFuture::getUninterruptibly );
    }

    public List<ResultSetFuture> createDataAsync(
            UUID entitySetId,
            UUID syncId,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType,
            Set<UUID> authorizedProperties,
            String entityId,
            SetMultimap<UUID, Object> entityDetails ) {
        List<ResultSetFuture> results = new ArrayList<ResultSetFuture>();
        
        // does not write the row if some property values that user is trying to write to are not authorized.
        if ( !authorizedProperties.containsAll( entityDetails.keySet() ) ) {
            logger.error( "Entity {} not written because not all property values are authorized.", entityId );
            return results;
        }

        SetMultimap<UUID, Object> normalizedPropertyValues = null;
        try {
            normalizedPropertyValues = CassandraSerDesFactory.validateFormatAndNormalize( entityDetails,
                    authorizedPropertiesWithDataType );
        } catch ( Exception e ) {
            logger.error( "Entity {} not written because some property values are of invalid format.",
                    entityId );
            return results;
        }

        // Stream<Entry<UUID, Object>> authorizedPropertyValues = propertyValues.entries().stream().filter( entry ->
        // authorizedProperties.contains( entry.getKey() ) );
        normalizedPropertyValues.entries().stream()
                .forEach( entry -> {
                    results.add( session.executeAsync(
                            writeDataQuery.bind()
                                    .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
                                    .setString( CommonColumns.ENTITYID.cql(), entityId )
                                    .setUUID( CommonColumns.SYNCID.cql(), syncId )
                                    .setUUID( CommonColumns.PROPERTY_TYPE_ID.cql(), entry.getKey() )
                                    .setBytes( CommonColumns.PROPERTY_VALUE.cql(),
                                            CassandraSerDesFactory.serializeValue(
                                                    mapper,
                                                    entry.getValue(),
                                                    authorizedPropertiesWithDataType
                                                            .get( entry.getKey() ),
                                                    entityId ) ) ) );
                } );

        Map<UUID, Object> normalizedPropertyValuesAsMap = normalizedPropertyValues.asMap().entrySet().stream()
                .collect( Collectors.toMap( e -> e.getKey(), e -> new HashSet<>( e.getValue() ) ) );

        eventBus.post( new EntityDataCreatedEvent(
                entitySetId,
                Optional.of( syncId ),
                entityId,
                normalizedPropertyValuesAsMap ) );
        
        return results;
    }

    public void createOrderedRPCData( UUID requestId, double weight, byte[] value ) {
        session.executeAsync( writeDataQuery.bind().setUUID( CommonColumns.RPC_REQUEST_ID.cql(), requestId )
                .setDouble( CommonColumns.RPC_WEIGHT.cql(), weight )
                .setBytes( CommonColumns.RPC_VALUE.cql(), ByteBuffer.wrap( value ) ) );
    }

    public Stream<byte[]> readNumRPCRows( UUID requestId, int numResults ) {
        logger.info( "Reading {} rows of RPC data for request id {}", numResults, requestId );
        BoundStatement bs = readNumRPCRowsQuery.bind().setUUID( CommonColumns.RPC_REQUEST_ID.cql(), requestId )
                .setInt( "numResults", numResults );
        ResultSet rs = session.execute( bs );
        return StreamUtil.stream( rs )
                .map( r -> r.getBytes( CommonColumns.RPC_VALUE.cql() ).array() );
    }

    /**
     * Delete data of an entity set across ALL sync Ids.
     * 
     * Note: this is currently only used when deleting an entity set, which takes care of deleting the data in
     * elasticsearch. If this is ever called without deleting the entity set, logic must be added to delete the data
     * from elasticsearch.
     */
    @SuppressFBWarnings(
        value = "UC_USELESS_OBJECT",
        justification = "results Object is used to execute deletes in batches" )
    public void deleteEntitySetData( UUID entitySetId ) {
        logger.info( "Deleting data of entity set: {}", entitySetId );
        BoundStatement bs = entityIdsQuery.bind().setUUID( CommonColumns.ENTITY_SET_ID.cql(),
                entitySetId );
        ResultSet rs = session.execute( bs );

        List<ResultSetFuture> results = new ArrayList<ResultSetFuture>();
        final int bufferSize = 1000;
        int counter = 0;

        for ( Row entityIdRow : rs ) {
            if ( counter > bufferSize ) {
                results.forEach( ResultSetFuture::getUninterruptibly );
                counter = 0;
                results = new ArrayList<ResultSetFuture>();
            }
            String entityId = RowAdapters.entityId( entityIdRow );

            results.add( asyncDeleteEntity( entitySetId, entityId ) );
            counter++;
        }

        results.forEach( ResultSetFuture::getUninterruptibly );
        logger.info( "Finish deletion of entity set data: {}", entitySetId );
    }

    public ResultSetFuture asyncDeleteEntity( UUID entitySetId, String entityId ) {
        return session.executeAsync( deleteEntityInAllSyncsQuery.bind()
                .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
                .setString( CommonColumns.ENTITYID.cql(), entityId ) );
    }

    public ResultSetFuture asyncDeleteEntity( UUID entitySetId, String entityId, UUID syncId ) {
        // load and delete, since Cassandra does not support delete by secondary index query
        ResultSetFuture rsf = asyncLoadEntity( entitySetId, entityId, syncId );
        Futures.addCallback( rsf, new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess( ResultSet rs ) {
                Row row = rs.one();
                if ( row != null ) {
                    session.execute( deleteEntityQuery.bind()
                            .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
                            .setString( CommonColumns.ENTITYID.cql(), entityId )
                            .setUUID( CommonColumns.SYNCID.cql(), syncId )
                            .setUUID( CommonColumns.PROPERTY_TYPE_ID.cql(), RowAdapters.propertyTypeId( row ) ) );
                }
            }

            @Override
            public void onFailure( Throwable t ) {
                logger.debug( "Loading for entity deletion failed: entitySetId {}, entityId {}, syncId {}",
                        entitySetId,
                        entityId,
                        syncId );
            }
        } );
        return rsf;
    }

    @Override
    public void deleteEntity( EntityKey entityKey ) {
        asyncDeleteEntity( entityKey.getEntitySetId(), entityKey.getEntityId(), entityKey.getSyncId() )
                .getUninterruptibly();
        eventBus.post( new EntityDataDeletedEvent(
                entityKey.getEntitySetId(),
                entityKey.getEntityId(),
                Optional.of( entityKey.getSyncId() ) ) );
    }

    private static PreparedStatement prepareEntitySetQuery(
            Session session,
            CassandraTableBuilder ctb ) {
        return session.prepare( entitySetQuery( ctb ) );
    }

    private static PreparedStatement prepareWriteQuery(
            Session session,
            CassandraTableBuilder ctb ) {
        return session.prepare( writeQuery( ctb ) );
    }

    private static Insert writeQuery( CassandraTableBuilder ctb ) {
        return ctb.buildStoreQuery();
    }

    private static Select.Where entitySetQuery( CassandraTableBuilder ctb ) {
        return ctb.buildLoadAllQuery().where( QueryBuilder
                .eq( CommonColumns.ENTITY_SET_ID.cql(), CommonColumns.ENTITY_SET_ID.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.ENTITYID.cql(), CommonColumns.ENTITYID.bindMarker() ) )
                .and( QueryBuilder.in( CommonColumns.PROPERTY_TYPE_ID.cql(),
                        CommonColumns.PROPERTY_TYPE_ID.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.SYNCID.cql(), CommonColumns.SYNCID.bindMarker() ) );
    }

    private static PreparedStatement prepareEntityIdsQuery( Session session ) {
        return session.prepare( QueryBuilder
                .select()
                .column( CommonColumns.ENTITY_SET_ID.cql() ).column( CommonColumns.ENTITYID.cql() )
                .distinct()
                .from( Table.DATA.getKeyspace(), Table.DATA.getName() )
                .where( QueryBuilder.eq( CommonColumns.ENTITY_SET_ID.cql(), QueryBuilder.bindMarker() ) ) );
    }

    private static PreparedStatement prepareLinkedEntitiesQuery( Session session ) {
        return session.prepare( QueryBuilder
                .select().all()
                .from( Table.LINKING_VERTICES.getKeyspace(), Table.LINKING_VERTICES.getName() ).allowFiltering()
                .where( QueryBuilder.eq( CommonColumns.GRAPH_ID.cql(), QueryBuilder.bindMarker() ) ) );
    }

    private static PreparedStatement prepareReadNumRPCRowsQuery( Session session ) {
        return session.prepare(
                QueryBuilder.select().from( Table.RPC_DATA_ORDERED.getKeyspace(), Table.RPC_DATA_ORDERED.getName() )
                        .where( QueryBuilder.eq( CommonColumns.RPC_REQUEST_ID.cql(),
                                CommonColumns.RPC_REQUEST_ID.bindMarker() ) )
                        .limit( QueryBuilder.bindMarker( "numResults" ) ) );
    }

    private static PreparedStatement prepareDeleteEntityInAllSyncsQuery(
            Session session ) {
        return session.prepare( Table.DATA.getBuilder().buildDeleteByPartitionKeyQuery() );
    }

    private static PreparedStatement prepareDeleteEntityQuery(
            Session session ) {
        return session.prepare( Table.DATA.getBuilder().buildDeleteQuery() );
    }

    /**
     * Auxiliary methods for linking entity sets
     */

    private Iterable<Pair<UUID, Set<EntityKey>>> getLinkedEntityKeys(
            UUID linkedEntitySetId ) {
        UUID graphId = linkingGraph.getGraphIdFromEntitySetId( linkedEntitySetId );
        ResultSet rs = session
                .execute( linkedEntitiesQuery.bind().setUUID( CommonColumns.GRAPH_ID.cql(), graphId ) );
        return Iterables.transform( rs, RowAdapters::linkedEntity );
    }

    private SetMultimap<FullQualifiedName, Object> getAndMergeLinkedEntities(
            UUID linkedEntitySetId,
            Pair<UUID, Set<EntityKey>> linkedKey,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesForEntitySets ) {
        SetMultimap<FullQualifiedName, Object> result = HashMultimap.create();
        SetMultimap<UUID, Object> indexResult = HashMultimap.create();

        linkedKey.getValue().stream()
                .map( key -> Pair.of( key.getEntitySetId(),
                        asyncLoadEntity( key.getEntitySetId(),
                                key.getEntityId(),
                                key.getSyncId(),
                                authorizedPropertyTypesForEntitySets.get( key.getEntitySetId() ).keySet() ) ) )
                .map( rsfPair -> Pair.of( rsfPair.getKey(), rsfPair.getValue().getUninterruptibly() ) )
                .map( rsPair -> RowAdapters.entityIdFQNPair( rsPair.getValue(),
                        authorizedPropertyTypesForEntitySets.get( rsPair.getKey() ),
                        mapper ) )
                .forEach( pair -> {
                    result.putAll( pair.getValue() );
                    indexResult.putAll( pair.getKey() );
                } );

        // Using HashSet here is necessary for serialization, to avoid kryo not knowing how to serialize guava
        // WrappedCollection
        Map<UUID, Object> indexResultAsMap = indexResult.asMap().entrySet().stream()
                .collect( Collectors.toMap( e -> e.getKey(), e -> new HashSet<>( e.getValue() ) ) );

        eventBus.post(
                new EntityDataCreatedEvent(
                        linkedEntitySetId,
                        Optional.absent(),
                        linkedKey.getKey().toString(),
                        indexResultAsMap ) );
        return result;
    }
}
