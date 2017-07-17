/*
 * Copyright (C) 2017. OpenLattice, Inc
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
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */

package com.dataloom.data.mapstores;

import static com.kryptnostic.datastore.cassandra.CommonColumns.ENTITYID;
import static com.kryptnostic.datastore.cassandra.CommonColumns.ENTITY_SET_ID;
import static com.kryptnostic.datastore.cassandra.CommonColumns.SYNCID;

import com.dataloom.data.EntityKey;
import com.dataloom.data.storage.CassandraEntityDatastore;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CassandraSerDesFactory;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.mapstores.SelfRegisteringMapStore;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class DataMapstore
        extends AbstractStructuredCassandraMapstore<EntityKey, SetMultimap<UUID, Object>> {
    private final PreparedStatement                readEntityKeysForEntitySetQuery;
    private final LoadingCache<UUID, PropertyType> propertyTypes;
    private final ObjectMapper                     mapper;

    public DataMapstore(
            String mapName,
            CassandraTableBuilder tableBuilder,
            Session session,
            SelfRegisteringMapStore<UUID, PropertyType> ptMapStore,
            ObjectMapper mapper ) {
        super( mapName, session, tableBuilder );
        this.readEntityKeysForEntitySetQuery = prepareReadEntityKeysForEntitySetQuery( session );
        this.propertyTypes = CacheBuilder.newBuilder().expireAfterAccess( 1, TimeUnit.MINUTES )
                .build( new CacheLoader<UUID, PropertyType>() {
                    @Override public PropertyType load( UUID key ) throws Exception {
                        return ptMapStore.load( key );
                    }
                } );
        this.mapper = mapper;
    }

    @Override public EntityKey generateTestKey() {
        return null;

    }

    @Override public SetMultimap<UUID, Object> generateTestValue() {
        return null;
    }

    @Override protected BoundStatement bind( EntityKey key, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ENTITY_SET_ID.cql(), key.getEntitySetId() )
                .setUUID( SYNCID.cql(), key.getSyncId() )
                .setString( CommonColumns.ENTITYID.cql(), key.getEntityId() );
    }

    @Override
    protected PreparedStatement prepareLoadQuery() {
        return session.prepare( Table.DATA.getBuilder().buildLoadAllQuery().where( CommonColumns.ENTITY_SET_ID.eq() )
                .and( SYNCID.eq() )
                .and( CassandraEntityDatastore.partitionIndexClause() )
                .and( CommonColumns.ENTITYID.eq() ) );

    }

    @Override protected PreparedStatement getLoadQuery() {
        return super.getLoadQuery();
    }

    @Override public void store( EntityKey key, SetMultimap<UUID, Object> value ) {
        throw new UnsupportedOperationException( "Data map store is read only!" );
    }

    @Override public void storeAll( Map<EntityKey, SetMultimap<UUID, Object>> map ) {
        throw new UnsupportedOperationException( "Data map store is read only!" );
    }

    @Override protected BoundStatement bind(
            EntityKey key, SetMultimap<UUID, Object> value, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ENTITY_SET_ID.cql(), key.getEntitySetId() )
                .setUUID( SYNCID.cql(), key.getSyncId() )
                .setString( CommonColumns.ENTITYID.cql(), key.getEntityId() );
    }

    @Override public Iterable<EntityKey> loadAllKeys() {
        return StreamUtil.stream( session
                .execute( currentSyncs( session ) ) )
                .parallel()
                .map( this::getEntityKeys )
                .map( ResultSetFuture::getUninterruptibly )
                .flatMap( StreamUtil::stream )
                .map( RowAdapters::entityKeyFromData )::iterator;
    }

    @Override protected EntityKey mapKey( Row rs ) {
        return RowAdapters.entityKey( rs );
    }

    @Override protected SetMultimap<UUID, Object> mapValue( ResultSet rs ) {
        final SetMultimap<UUID, Object> m = HashMultimap.create();
        for ( Row row : rs ) {
            UUID propertyTypeId = row.getUUID( CommonColumns.PROPERTY_TYPE_ID.cql() );
            String entityId = row.getString( CommonColumns.ENTITYID.cql() );
            if ( propertyTypeId != null ) {
                PropertyType pt = propertyTypes.getUnchecked( propertyTypeId );
                m.put( propertyTypeId,
                        CassandraSerDesFactory.deserializeValue( mapper,
                                row.getBytes( CommonColumns.PROPERTY_BUFFER.cql() ),
                                pt.getDatatype(),
                                entityId ) );
            }
        }
        return m;
    }

    public ResultSetFuture getEntityKeys( Row row ) {
        final UUID entitySetId = RowAdapters.entitySetId( row );
        final UUID syncId = RowAdapters.currentSyncId( row );
        return session.executeAsync( readEntityKeysForEntitySetQuery.bind()
                .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
                .setUUID( SYNCID.cql(), syncId ) );
    }

    @Override public MapStoreConfig getMapStoreConfig() {
        return super.getMapStoreConfig().setInitialLoadMode( InitialLoadMode.EAGER );
    }

    @Override public MapConfig getMapConfig() {
        return super.getMapConfig()
                .setInMemoryFormat( InMemoryFormat.OBJECT );
    }

    public static Select currentSyncs( Session session ) {
        return QueryBuilder.select( CommonColumns.ENTITY_SET_ID.cql(), CommonColumns.CURRENT_SYNC_ID.cql() )
                .distinct().from( Table.SYNC_IDS.getKeyspace(), Table.SYNC_IDS.getName() );

    }

    public static PreparedStatement prepareReadEntityKeysForEntitySetQuery( Session session ) {
        return session.prepare( QueryBuilder.select( ENTITY_SET_ID.cql(), SYNCID.cql(), ENTITYID.cql() )
                .from( Table.DATA.getKeyspace(), Table.DATA.getName() )
                .where( ENTITY_SET_ID.eq() )
                .and( SYNCID.eq() )
                .and( CassandraEntityDatastore.partitionIndexClause() ) );
    }

}
