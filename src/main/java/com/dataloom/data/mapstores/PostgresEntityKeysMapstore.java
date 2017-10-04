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

import com.dataloom.data.EntityKey;
import com.dataloom.graph.mapstores.KeyIterator;
import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.streams.StreamUtil;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.kryptnostic.rhizome.mapstores.TestableSelfRegisteringMapStore;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class PostgresEntityKeysMapstore implements TestableSelfRegisteringMapStore<UUID, EntityKey> {
    private static final Logger logger        = LoggerFactory.getLogger( PostgresEntityKeyIdsMapstore.class );
    private static final String CREATE_TABLE  =
            "CREATE TABLE IF NOT EXISTS ids ( entity_set_id UUID, syncid UUID, entityid TEXT, id UUID,"
                    + "PRIMARY KEY(entity_set_id , syncid , entityid ), UNIQUE(ID) )";
    private static final String SELECT_ROW    = "SELECT * from ids where id = ?";
    private static final String DELETE_ROW    = "DELETE from edges where id = ?";
    private static final String LOAD_ALL_KEYS = "select id from edges";
    private final String           mapName;
    private final HikariDataSource hds;

    public PostgresEntityKeysMapstore( String mapName, HikariDataSource hds ) throws SQLException {
        this.mapName = mapName;
        this.hds = hds;
        Connection connection = hds.getConnection();
        connection.createStatement().execute( CREATE_TABLE );
        connection.close();
        logger.info( "Initialized Postgres Entity Keys Mapstore" );
    }

    @Override public String getMapName() {
        return mapName;
    }

    @Override public String getTable() {
        return null;
    }

    @Override
    public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override
    public EntityKey generateTestValue() {
        return TestDataFactory.entityKey();
    }

    @Override public void store( UUID key, EntityKey value ) {
        throw new UnsupportedOperationException( "Entity keys mapstore is over a materialized view." );
    }

    @Override public void storeAll( Map<UUID, EntityKey> map ) {
        throw new UnsupportedOperationException( "Entity keys mapstore is over a materialized view." );
    }

    @Override public void delete( UUID key ) {
        throw new UnsupportedOperationException( "Entity keys mapstore is over a materialized view." );
    }

    @Override public void deleteAll( Collection<UUID> keys ) {
        throw new UnsupportedOperationException( "Entity keys mapstore is over a materialized view." );
    }

    @Override
    public MapStoreConfig getMapStoreConfig() {
        return new MapStoreConfig()
                .setImplementation( this )
                .setEnabled( true )
                .setWriteDelaySeconds( 0 );
    }

    @Override
    public MapConfig getMapConfig() {
        return new MapConfig( mapName )
                .setMapStoreConfig( getMapStoreConfig() );
    }

    @Override public EntityKey load( UUID key ) {
        EntityKey val = null;
        try ( Connection connection = hds.getConnection() ) {
            PreparedStatement selectRow = connection.prepareStatement( SELECT_ROW );
            bind( selectRow, key );
            ResultSet rs = selectRow.executeQuery();
            if ( rs.next() ) {
                val = mapToValue( rs );
            }
            logger.info( "LOADED: {}", val );
        } catch ( SQLException e ) {
            logger.error( "Error executing SQL during select for key {}.", key, e );
        }
        return val;
    }

    @Override public Map<UUID, EntityKey> loadAll( Collection<UUID> keys ) {
        return keys.parallelStream().collect( Collectors.toConcurrentMap( Function.identity(), this::load ) );
    }

    @Override public Iterable<UUID> loadAllKeys() {
        logger.info( "Starting load all keys for Edge Mapstore" );
        Stream<UUID> keys;
        try {
            final Connection connection = hds.getConnection();
            final ResultSet rs = connection.createStatement().executeQuery( LOAD_ALL_KEYS );
            return StreamUtil
                    .stream( () -> new KeyIterator<>( rs,
                            new CountdownConnectionCloser( connection, 1 ),
                            PostgresEntityKeysMapstore::mapToKey ) )
                    .peek( key -> logger.info( "Key to load: {}", key ) )
                    ::iterator;
        } catch ( SQLException e ) {
            logger.error( "Unable to acquire connection load all keys" );
            return null;
        }
    }

    public static UUID mapToKey( java.sql.ResultSet rs ) {
        try {
            return (UUID) rs.getObject( "id" );
        } catch ( SQLException e ) {
            logger.error( "Unable to map to value.", e );
            return null;
        }
    }

    public static EntityKey mapToValue( java.sql.ResultSet rs ) {
        try {
            UUID entitySetId = (UUID) rs.getObject( "entity_set_id" );
            UUID syncId = (UUID) rs.getObject( "syncid" );
            String entityId = rs.getString( "entityid" );
            return new EntityKey( entitySetId, entityId, syncId );
        } catch ( SQLException e ) {
            logger.error( "Unable to map data key.", e );
            return null;
        }
    }

    public static void bind( PreparedStatement ps, UUID key ) throws SQLException {
        ps.setObject( 1, key );
    }
}
