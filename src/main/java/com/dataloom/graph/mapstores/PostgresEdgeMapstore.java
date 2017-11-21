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

import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.graph.edge.LoomEdge;
import com.dataloom.hazelcast.HazelcastMap;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.ResultSetAdapters;
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PostgresEdgeMapstore extends AbstractBasePostgresMapstore<EdgeKey, LoomEdge> {
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

    public PostgresEdgeMapstore( HikariDataSource hds ) throws SQLException {
        super( HazelcastMap.EDGES.name(), PostgresTable.EDGES, hds );
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
        return super.getMapStoreConfig()
                .setImplementation( this )
                .setInitialLoadMode( InitialLoadMode.EAGER )
                .setEnabled( true )
                .setWriteDelaySeconds( 5 );
    }

    @Override public MapConfig getMapConfig() {
        return super.getMapConfig()
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

    public void bind( PreparedStatement ps, EdgeKey key ) throws SQLException {
        ps.setObject( 1, key.getSrcEntityKeyId() );
        ps.setObject( 2, key.getDstTypeId() );
        ps.setObject( 3, key.getEdgeTypeId() );
        ps.setObject( 4, key.getDstEntityKeyId() );
        ps.setObject( 5, key.getEdgeEntityKeyId() );
    }

    public void bind( PreparedStatement ps, EdgeKey key, LoomEdge value ) throws SQLException {
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

    public LoomEdge mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.loomEdge( rs );
    }

    public EdgeKey mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.edgeKey( rs );
    }

}
