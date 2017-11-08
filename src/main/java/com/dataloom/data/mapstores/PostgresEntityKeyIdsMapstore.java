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
import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.streams.StreamUtil;
import com.google.common.collect.Lists;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.kryptnostic.rhizome.mapstores.SelfRegisteringMapStore;
import com.kryptnostic.rhizome.mapstores.TestableSelfRegisteringMapStore;
import com.openlattice.postgres.CountdownConnectionCloser;
import com.openlattice.postgres.KeyIterator;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PostgresEntityKeyIdsMapstore implements TestableSelfRegisteringMapStore<EntityKey, UUID> {
    private static final Logger logger        = LoggerFactory.getLogger( PostgresEntityKeyIdsMapstore.class );
    private static final String CREATE_TABLE  =
            "CREATE TABLE IF NOT EXISTS ids ( entity_set_id UUID, syncid UUID, entityid TEXT, id UUID,"
                    + "PRIMARY KEY(entity_set_id , syncid , entityid ), UNIQUE(ID) )";
    private static final String INSERT_ROW    = "INSERT INTO ids VALUES(?,?,?,?) on conflict do nothing";
    private static final String SELECT_ROW    = "SELECT * from ids where entity_set_id = ? and syncid = ? and entityid = ?";
    private static final String DELETE_ROW    = "DELETE from edges where entity_set_id = ? and syncid = ? and entityid = ?";
    private static final String LOAD_ALL_KEYS = "select entity_set_id ,syncid, entityid from edges";
    private final String           mapName;
    private final HikariDataSource hds;
    SelfRegisteringMapStore<UUID, EntityKey> ekm;

    public PostgresEntityKeyIdsMapstore(
            String mapName,
            HikariDataSource hds,
            SelfRegisteringMapStore<UUID, EntityKey> ekm ) throws SQLException {
        this.mapName = mapName;
        this.hds = hds;
        this.ekm = ekm;
        Connection connection = hds.getConnection();
        connection.createStatement().execute( CREATE_TABLE );
        connection.close();
        logger.info( "Initialized Postgres Entity Key Ids Mapstore" );
    }

    @Override
    public String getMapName() {
        return mapName;
    }

    @Override
    public String getTable() {
        return null;
    }

    @Override
    public EntityKey generateTestKey() {
        return TestDataFactory.entityKey();
    }

    @Override
    public UUID generateTestValue() {
        return UUID.randomUUID();
    }

    @Override
    public MapStoreConfig getMapStoreConfig() {
        return new MapStoreConfig()
                .setInitialLoadMode( InitialLoadMode.EAGER )
                .setImplementation( this )
                .setEnabled( true )
                .setWriteDelaySeconds( 5 );
    }

    @Override
    public MapConfig getMapConfig() {
        return new MapConfig( mapName )
                .setMapStoreConfig( getMapStoreConfig() )
                .addMapIndexConfig( new MapIndexConfig( DataMapstore.KEY_ENTITY_SET_ID, false ) )
                .addMapIndexConfig( new MapIndexConfig( DataMapstore.KEY_SYNC_ID, false ) );
    }

    @Override
    public void store( EntityKey key, UUID value ) {
        try ( Connection connection = hds.getConnection() ) {
            PreparedStatement insertRow = connection.prepareStatement( INSERT_ROW );
            bind( insertRow, key, value );
            insertRow.executeUpdate();
        } catch ( SQLException e ) {
            logger.error( "Error executing SQL during store for key {}.", key, e );
        }
    }

    @Override
    public void storeAll( Map<EntityKey, UUID> map ) {
        EntityKey key = null;
        try ( Connection connection = hds.getConnection() ) {
            PreparedStatement insertRow = connection.prepareStatement( INSERT_ROW );
            connection.setAutoCommit( false );
            for ( Entry<EntityKey, UUID> entry : map.entrySet() ) {
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

    @Override
    public void delete( EntityKey key ) {
        try ( Connection connection = hds.getConnection() ) {
            PreparedStatement deleteRow = connection.prepareStatement( DELETE_ROW );
            bind( deleteRow, key );
            deleteRow.executeUpdate();
            connection.close();
        } catch ( SQLException e ) {
            logger.error( "Error executing SQL during delete for key {}.", key, e );
        }
    }

    @Override public void deleteAll( Collection<EntityKey> keys ) {
        EntityKey key = null;
        try ( Connection connection = hds.getConnection() ) {
            PreparedStatement deleteRow = connection.prepareStatement( DELETE_ROW );
            connection.setAutoCommit( false );
            for ( EntityKey entry : keys ) {
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

    @Override public UUID load( EntityKey key ) {
        UUID id = tryLoad( key );

        if ( id != null ) {
            return id;
        }

        do {
            id = UUID.randomUUID();
        } while ( ekm.load( id ) != null );

        store( key, id );

        return id;
    }

    public UUID tryLoad( EntityKey key ) {
        UUID val = null;
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

    @Override public Map<EntityKey, UUID> loadAll( Collection<EntityKey> keys ) {
        return keys.parallelStream().collect( Collectors.toConcurrentMap( Function.identity(), this::load ) );
    }

    @Override public Iterable<EntityKey> loadAllKeys() {
        logger.info( "Starting load all keys for Edge Mapstore" );
        Stream<EntityKey> keys;
        try ( Connection connection = hds.getConnection() ) {
            final ResultSet rs = connection.createStatement().executeQuery( LOAD_ALL_KEYS );
            return StreamUtil
                    .stream( () -> new KeyIterator<>( rs,
                            new CountdownConnectionCloser( connection, 1 ),
                            PostgresEntityKeyIdsMapstore::mapToKey ) )
                    .peek( key -> logger.info( "Key to load: {}", key ) )
                    ::iterator;
        } catch ( SQLException e ) {
            logger.error( "Unable to acquire connection load all keys" );
            return null;
        }
    }

    public static void bind( PreparedStatement ps, EntityKey key ) throws SQLException {
        ps.setObject( 1, key.getEntitySetId() );
        ps.setObject( 2, key.getSyncId() );
        ps.setObject( 3, key.getEntityId() );
    }

    public static void bind( PreparedStatement ps, EntityKey key, UUID value ) throws SQLException {
        bind( ps, key );
        ps.setObject( 4, value );
    }

    public static UUID mapToValue( ResultSet rs ) {
        try {
            return (UUID) rs.getObject( "id" );
        } catch ( SQLException e ) {
            logger.error( "Unable to map to value.", e );
            return null;
        }
    }

    public static EntityKey mapToKey( ResultSet rs ) {
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
}
