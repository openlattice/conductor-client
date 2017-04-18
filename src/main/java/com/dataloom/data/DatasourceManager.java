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

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.lang3.NotImplementedException;

import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.sync.events.CurrentSyncUpdatedEvent;
import com.dataloom.sync.events.SyncIdCreatedEvent;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
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

public class DatasourceManager {

    @Inject
    private EventBus                eventBus;

    private final Session           session;

    private final IMap<UUID, UUID>  currentSyncIds;

    private final PreparedStatement mostRecentSyncIdQuery;
    private final PreparedStatement writeSyncIdsQuery;
    private final PreparedStatement allPreviousSyncIdsQuery;

    public DatasourceManager( Session session, HazelcastInstance hazelcastInstance ) {
        this.session = session;

        this.currentSyncIds = hazelcastInstance.getMap( HazelcastMap.SYNC_IDS.name() );

        CassandraTableBuilder syncTableDefinitions = Table.SYNC_IDS.getBuilder();

        this.mostRecentSyncIdQuery = prepareMostRecentSyncIdQuery( session );
        this.writeSyncIdsQuery = prepareWriteQuery( session, syncTableDefinitions );
        this.allPreviousSyncIdsQuery = prepareAllPreviousSyncIdsQuery( session );
    }

    public UUID getCurrentSyncId( UUID entitySetId ) {
        return currentSyncIds.get( entitySetId );
    }
    
    public Map<UUID, UUID> getCurrentSyncId( Set<UUID> entitySetIds ){
        return currentSyncIds.getAll( entitySetIds );
    }

    public void setCurrentSyncId( UUID entitySetId, UUID syncId ) {
        currentSyncIds.put( entitySetId, syncId );
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

    private void addSyncIdToEntitySet( UUID entitySetId, UUID syncId ) {
        session.execute( writeSyncIdsQuery.bind().setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
                .setUUID( CommonColumns.SYNCID.cql(), syncId ) );
    }
}
