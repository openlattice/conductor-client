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

package com.dataloom.data;

import com.codahale.metrics.annotation.Timed;
import com.dataloom.data.mapstores.DataMapstore;
import com.dataloom.data.storage.EntityBytes;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.neuron.audit.AuditEntitySetUtils;
import com.dataloom.streams.StreamUtil;
import com.dataloom.sync.events.CurrentSyncUpdatedEvent;
import com.dataloom.sync.events.SyncIdCreatedEvent;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

public class DatasourceManager {
    private static final Logger logger = LoggerFactory.getLogger( DatasourceManager.class );
    private final Session                 session;
    private final IMap<UUID, UUID>        currentSyncIds;
    private final PreparedStatement       mostRecentSyncIdQuery;
    private final PreparedStatement       writeSyncIdsQuery;
    private final PreparedStatement       allPreviousSyncIdsQuery;
    private final PreparedStatement       allPreviousEntitySetsAndSyncIdsQuery;
    private final IMap<UUID, EntityBytes> data;

    @Inject
    private EventBus eventBus;

    public DatasourceManager( Session session, HazelcastInstance hazelcastInstance ) {
        this.session = session;

        this.currentSyncIds = hazelcastInstance.getMap( HazelcastMap.SYNC_IDS.name() );
        this.data = hazelcastInstance.getMap( HazelcastMap.DATA.name() );
        CassandraTableBuilder syncTableDefinitions = Table.SYNC_IDS.getBuilder();

        this.mostRecentSyncIdQuery = prepareMostRecentSyncIdQuery( session );
        this.writeSyncIdsQuery = prepareWriteQuery( session, syncTableDefinitions );
        this.allPreviousSyncIdsQuery = prepareAllPreviousSyncIdsQuery( session );
        this.allPreviousEntitySetsAndSyncIdsQuery = prepareAllPreviousEntitySetsSyncIdsQuery( session );
    }

    public UUID getCurrentSyncId( UUID entitySetId ) {
        return currentSyncIds.get( entitySetId );
    }

    public Map<UUID, UUID> getCurrentSyncId( Set<UUID> entitySetIds ) {
        return currentSyncIds.getAll( entitySetIds );
    }

    public void setCurrentSyncId( UUID entitySetId, UUID syncId ) {

        currentSyncIds.put( entitySetId, syncId );

        if ( entitySetId.equals( AuditEntitySetUtils.getId() ) ) {
            AuditEntitySetUtils.setSyncId( syncId );
        }

        eventBus.post( new CurrentSyncUpdatedEvent( entitySetId, syncId ) );
    }

    public UUID createNewSyncIdForEntitySet( UUID entitySetId ) {
        UUID newSyncId = UUIDs.timeBased();
        addSyncIdToEntitySet( entitySetId, newSyncId );
        eventBus.post( new SyncIdCreatedEvent( entitySetId, newSyncId ) );
        return newSyncId;
    }

    public UUID getLatestSyncId( UUID entitySetId ) {
        BoundStatement bs = mostRecentSyncIdQuery.bind().setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId );
        Row row = session.execute( bs ).one();
        return ( row == null ) ? null : RowAdapters.syncId( row );
    }

    public Iterable<UUID> getAllPreviousSyncIds( UUID entitySetId, UUID syncId ) {
        ResultSet rs = session
                .execute( allPreviousSyncIdsQuery.bind().setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
                        .setUUID( CommonColumns.SYNCID.cql(), syncId ) );
        return Iterables.transform( rs, RowAdapters::syncId );
    }

    public Iterable<UUID> getAllSyncIds( UUID entitySetId ) {
        ResultSet rs = session
                .execute( allPreviousSyncIdsQuery.bind().setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId ) );
        return Iterables.transform( rs, RowAdapters::syncId );
    }

    private void addSyncIdToEntitySet( UUID entitySetId, UUID syncId ) {
        session.execute( writeSyncIdsQuery.bind().setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
                .setUUID( CommonColumns.SYNCID.cql(), syncId ) );
    }

    @Scheduled( fixedRate = 60000 )
    public void reap() {
        logger.info( "Reaping old syncs from memory" );
        cleanup();
    }

    @Timed
    public void cleanup() {
        //This will evict as opposed to remove all items.
        StreamUtil.stream( session.execute( DataMapstore.currentSyncs() ) )
                .map( row -> {
                    UUID entitySetId = RowAdapters.entitySetId( row );
                    UUID syncId = RowAdapters.currentSyncId( row );
                    return session.executeAsync( allPreviousEntitySetsAndSyncIdsQuery.bind()
                            .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
                            .setUUID( CommonColumns.SYNCID.cql(), syncId ) );
                } )
                .map( ResultSetFuture::getUninterruptibly )
                .flatMap( StreamUtil::stream )
                .map( EntitySets::filterByEntitySetIdAndSyncId )
                .map( data::keySet )
                .flatMap( Set::stream )
                .forEach( data::evict );
    }

    private static PreparedStatement prepareAllPreviousEntitySetsSyncIdsQuery( Session session ) {
        return session.prepare( QueryBuilder.select()
                .column( CommonColumns.ENTITY_SET_ID.cql() )
                .column( CommonColumns.SYNCID.cql() )
                .from( Table.SYNC_IDS.getKeyspace(), Table.SYNC_IDS.getName() )
                .where( QueryBuilder.eq( CommonColumns.ENTITY_SET_ID.cql(), CommonColumns.ENTITY_SET_ID.bindMarker() ) )
                .and( QueryBuilder.lt( CommonColumns.SYNCID.cql(), CommonColumns.SYNCID.bindMarker() ) ) );
    }

    private static PreparedStatement prepareWriteQuery(
            Session session,
            CassandraTableBuilder ctb ) {
        return session.prepare( ctb.buildStoreQuery() );
    }

    private static PreparedStatement prepareMostRecentSyncIdQuery( Session session ) {
        return session
                .prepare( QueryBuilder.select().all().from( Table.SYNC_IDS.getKeyspace(), Table.SYNC_IDS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_SET_ID.cql(),
                                CommonColumns.ENTITY_SET_ID.bindMarker() ) )
                        .limit( 1 ) );
    }

    private static PreparedStatement prepareAllPreviousSyncIdsQuery( Session session ) {
        return session.prepare( QueryBuilder.select().column( CommonColumns.SYNCID.cql() )
                .from( Table.SYNC_IDS.getKeyspace(), Table.SYNC_IDS.getName() )
                .where( QueryBuilder.eq( CommonColumns.ENTITY_SET_ID.cql(), CommonColumns.ENTITY_SET_ID.bindMarker() ) )
                .and( QueryBuilder.lt( CommonColumns.SYNCID.cql(), CommonColumns.SYNCID.bindMarker() ) ) );
    }
}
