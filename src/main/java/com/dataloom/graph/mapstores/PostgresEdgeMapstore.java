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

package com.dataloom.graph.mapstores;

import com.dataloom.data.mapstores.CountdownConnectionCloser;
import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.graph.edge.LoomEdge;
import com.dataloom.streams.StreamUtil;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.kryptnostic.rhizome.mapstores.TestableSelfRegisteringMapStore;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PostgresEdgeMapstore implements TestableSelfRegisteringMapStore<EdgeKey, LoomEdge> {
    public static final  String EDGE_SET_ID       = "edgeSetId";
    public static final  String SRC_ENTITY_KEY_ID = "srcEntityKeyId";
    public static final  String DST_ENTITY_KEY_ID = "dstEntityKeyId";
    public static final  String SRC_SET_ID        = "srcSetId";
    public static final  String DST_SET_ID        = "dstSetId";
    private static final Logger logger            = LoggerFactory.getLogger( PostgresEdgeMapstore.class );
    private static final String CREATE_TABLE      = "CREATE TABLE IF NOT EXISTS edges ("
            + "src_entity_key_id UUID, src_type_id UUID, src_entity_set_id UUID, src_sync_id UUID,"
            + "dst_entity_key_id UUID, dst_type_id UUID, dst_entity_set_id UUID, dst_sync_id UUID,"
            + "edge_entity_key_id UUID, edge_type_id UUID, edge_entity_set_id UUID,"
            + "PRIMARY KEY(src_entity_key_id,dst_type_id,edge_type_id,dst_entity_key_id,edge_entity_key_id) )";
    private static final String INSERT_ROW        = "INSERT INTO edges VALUES(?,?,?,?,?,?,?,?,?,?,?) on conflict do nothing";
    private static final String SELECT_ROW        = "SELECT * from edges where src_entity_key_id = ? and dst_type_id = ? and edge_type_id=? and dst_entity_key_id = ? and edge_entity_key_id = ?";
    private static final String DELETE_ROW        = "DELETE from edges where src_entity_key_id = ? and dst_type_id = ? and edge_type_id=? and dst_entity_key_id = ? and edge_entity_key_id = ?";
    private static final String LOAD_ALL_KEYS     = "select src_entity_key_id,dst_type_id,edge_type_id,dst_entity_key_id,edge_entity_key_id from edges";
    private final String           mapName;
    private final HikariDataSource hds;

    public PostgresEdgeMapstore( String mapName, HikariDataSource hds ) throws SQLException {
        this.mapName = mapName;
        this.hds = hds;
        Connection connection = hds.getConnection();
        connection.createStatement().execute( CREATE_TABLE );
        connection.close();
        logger.info( "Initialized Postgres Edge Mapstore" );
    }

    @Override public String getMapName() {
        return mapName;
    }

    @Override public String getTable() {
        return null;
    }

    @Override public EdgeKey generateTestKey() {
        return new EdgeKey( new UUID( 0, 0 ),
                new UUID( 0, 1 ),
                new UUID( 0, 2 ),
                new UUID( 0, 3 ),
                new UUID( 0, 4 ) );
    }

    @Override public LoomEdge generateTestValue() {
        return new LoomEdge( generateTestKey(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID() );
    }

    @Override public MapStoreConfig getMapStoreConfig() {
        return new MapStoreConfig()
                .setImplementation( this )
                .setInitialLoadMode( InitialLoadMode.EAGER )
                .setEnabled( true )
                .setWriteDelaySeconds( 5 );
    }

    @Override public MapConfig getMapConfig() {
        return new MapConfig( mapName )
                .setMapStoreConfig( getMapStoreConfig() )
                .setInMemoryFormat( InMemoryFormat.OBJECT )
                .addMapIndexConfig( new MapIndexConfig( SRC_ENTITY_KEY_ID, false ) )
                .addMapIndexConfig( new MapIndexConfig( DST_ENTITY_KEY_ID, false ) )
                .addMapIndexConfig( new MapIndexConfig( "dstTypeId", false ) )
                .addMapIndexConfig( new MapIndexConfig( DST_SET_ID, false ) )
                .addMapIndexConfig( new MapIndexConfig( "dstSyncId", false ) )
                .addMapIndexConfig( new MapIndexConfig( "srcTypeId", false ) )
                .addMapIndexConfig( new MapIndexConfig( SRC_SET_ID, false ) )
                .addMapIndexConfig( new MapIndexConfig( "srcSyncId", false ) )
                .addMapIndexConfig( new MapIndexConfig( "edgeTypeId", false ) )
                .addMapIndexConfig( new MapIndexConfig( EDGE_SET_ID, false ) );
    }

    @Override public void store( EdgeKey key, LoomEdge value ) {
        try ( Connection connection = hds.getConnection() ) {
            PreparedStatement insertRow = connection.prepareStatement( INSERT_ROW );
            bind( insertRow, key, value );
            insertRow.executeUpdate();
        } catch ( SQLException e ) {
            logger.error( "Error executing SQL during store for key {}.", key, e );
        }
    }

    @Override public void storeAll( Map<EdgeKey, LoomEdge> map ) {
        EdgeKey key = null;
        try ( Connection connection = hds.getConnection() ) {
            PreparedStatement insertRow = connection.prepareStatement( INSERT_ROW );
            connection.setAutoCommit( false );
            for ( Entry<EdgeKey, LoomEdge> entry : map.entrySet() ) {
                key = entry.getKey();
                bind( insertRow, key, entry.getValue() );
                insertRow.executeUpdate();
            }
            connection.commit();
            connection.setAutoCommit( true );
            connection.close();
        } catch ( SQLException e ) {
            logger.error( "Error executing SQL during store all for key {}", key, e );
        }
    }

    @Override public void delete( EdgeKey key ) {
        try ( Connection connection = hds.getConnection() ) {
            PreparedStatement deleteRow = connection.prepareStatement( DELETE_ROW );
            bind( deleteRow, key );
            deleteRow.executeUpdate();
            connection.close();
        } catch ( SQLException e ) {
            logger.error( "Error executing SQL during delete for key {}.", key, e );
        }
    }

    @Override public void deleteAll( Collection<EdgeKey> keys ) {
        EdgeKey key = null;
        try {
            Connection connection = hds.getConnection();
            PreparedStatement deleteRow = connection.prepareStatement( DELETE_ROW );
            connection.setAutoCommit( false );
            for ( EdgeKey entry : keys ) {
                key = entry;
                bind( deleteRow, key );
                deleteRow.executeUpdate();
            }
            connection.commit();
            connection.setAutoCommit( true );
            connection.close();
        } catch ( SQLException e ) {
            logger.error( "Error executing SQL during delete all for key {}", key, e );
        }
    }

    @Override public LoomEdge load( EdgeKey key ) {
        LoomEdge val = null;
        try ( Connection connection = hds.getConnection() ) {
            PreparedStatement selectRow = connection.prepareStatement( SELECT_ROW );
            bind( selectRow, key );
            ResultSet rs = selectRow.executeQuery();
            if ( rs.next() ) {
                val = mapToValue( rs );
            }
            logger.debug( "LOADED: {}", val );
        } catch ( SQLException e ) {
            logger.error( "Error executing SQL during select for key {}.", key, e );
        }
        return val;
    }

    @Override public Map<EdgeKey, LoomEdge> loadAll( Collection<EdgeKey> keys ) {
        return keys.parallelStream().collect( Collectors.toConcurrentMap( Function.identity(), this::load ) );
    }

    @Override public Iterable<EdgeKey> loadAllKeys() {
        logger.info( "Starting load all keys for Edge Mapstore" );
        Stream<EdgeKey> keys;
        try {
            final Connection connection = hds.getConnection();
            final ResultSet rs = connection.createStatement().executeQuery( LOAD_ALL_KEYS );
            return StreamUtil
                    .stream( () -> new KeyIterator<>( rs,
                            new CountdownConnectionCloser( connection, 1 ),
                            PostgresEdgeMapstore::mapToKey ) )
                    .peek( key -> logger.debug( "Key to load: {}", key ) )
                    ::iterator;
        } catch ( SQLException e ) {
            logger.error( "Unable to acquire connection load all keys" );
            return null;
        }
    }

    public static void bind( PreparedStatement ps, EdgeKey key ) throws SQLException {
        ps.setObject( 1, key.getSrcEntityKeyId() );
        ps.setObject( 2, key.getDstTypeId() );
        ps.setObject( 3, key.getEdgeTypeId() );
        ps.setObject( 4, key.getDstEntityKeyId() );
        ps.setObject( 5, key.getEdgeEntityKeyId() );
    }

    public static void bind( PreparedStatement ps, EdgeKey key, LoomEdge value ) throws SQLException {
        ps.setObject( 1, value.getSrcEntityKeyId() );
        ps.setObject( 2, value.getSrcTypeId() );
        ps.setObject( 3, value.getSrcSetId() );
        ps.setObject( 4, value.getSrcSyncId() );
        ps.setObject( 5, value.getDstEntityKeyId() );
        ps.setObject( 6, value.getDstTypeId() );
        ps.setObject( 7, value.getDstSetId() );
        ps.setObject( 8, value.getDstSyncId() );
        ps.setObject( 9, value.getEdgeEntityKeyId() );
        ps.setObject( 10, value.getEdgeTypeId() );
        ps.setObject( 11, value.getEdgeSetId() );
    }

    public static LoomEdge mapToValue( ResultSet rs ) {
        try {
            EdgeKey key = mapToKey( rs );
            UUID srcType = (UUID) rs.getObject( "src_type_id" );
            UUID srcSetId = (UUID) rs.getObject( "src_entity_set_id" );

            UUID srcSyncId = (UUID) rs.getObject( "src_sync_id" );
            UUID dstSetId = (UUID) rs.getObject( "dst_entity_set_id" );
            UUID dstSyncId = (UUID) rs.getObject( "dst_sync_id" );
            UUID edgeSetId = (UUID) rs.getObject( "edge_entity_set_id" );
            return new LoomEdge( key, srcType, srcSetId, srcSyncId, dstSetId, dstSyncId, edgeSetId );
        } catch ( SQLException e ) {
            logger.error( "Unable to map to value.", e );
            return null;
        }
    }

    public static EdgeKey mapToKey( ResultSet rs ) {
        try {
            UUID srcEntityKeyId = (UUID) rs.getObject( "src_entity_key_id" );
            UUID dstTypeId = (UUID) rs.getObject( "dst_type_id" );
            UUID edgeTypeId = (UUID) rs.getObject( "edge_type_id" );
            UUID dstEntityKeyId = (UUID) rs.getObject( "dst_entity_key_id" );
            UUID edgeEntityKeyId = (UUID) rs.getObject( "edge_entity_key_id" );
            return new EdgeKey( srcEntityKeyId, dstTypeId, edgeTypeId, dstEntityKeyId, edgeEntityKeyId );
        } catch ( SQLException e ) {
            logger.error( "Unable to map data key.", e );
            return null;
        }
    }

}
