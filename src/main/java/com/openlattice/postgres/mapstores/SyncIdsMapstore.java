package com.openlattice.postgres.mapstores;

import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableList;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.openlattice.postgres.PostgresColumn.*;
import static com.openlattice.postgres.PostgresTable.SYNC_IDS;

public class SyncIdsMapstore extends AbstractBasePostgresMapstore<UUID, UUID> {
    private final HikariDataSource hds;
    private final String updateSql = "UPDATE ".concat( SYNC_IDS.getName() ).concat( " SET " )
            .concat( CURRENT_SYNC_ID.getName() ).concat( " = ? WHERE " ).concat( ENTITY_SET_ID.getName() )
            .concat( " = ?;" );

    public SyncIdsMapstore( HikariDataSource hds ) {
        super( HazelcastMap.SYNC_IDS.name(), SYNC_IDS, hds );
        this.hds = hds;
    }

    @Override protected List<PostgresColumnDefinition> keyColumns() {
        return ImmutableList.of( ENTITY_SET_ID );
    }

    @Override protected List<PostgresColumnDefinition> valueColumns() {
        return ImmutableList.of( SYNC_ID, CURRENT_SYNC_ID );
    }

    @Override protected void bind( PreparedStatement ps, UUID key, UUID value ) throws SQLException {
        ps.setObject( 1, value );
        ps.setObject( 2, key );
    }

    @Override protected void bind( PreparedStatement ps, UUID key ) throws SQLException {
        ps.setObject( 1, key );
    }

    @Override protected UUID mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.currentSyncId( rs );
    }

    @Override protected UUID mapToKey( ResultSet rs ) {
        try {
            return ResultSetAdapters.entitySetId( rs );
        } catch ( SQLException e ) {
            logger.debug( "Unable to map result set to entitySetId", e );
            return null;
        }
    }

    // This mapstore should never add rows to the existing table -- instead it should just update
    // existing rows when a new currentSyncId is set for an entity set

    @Override
    public void store( UUID key, UUID value ) {
        try ( Connection connection = hds.getConnection() ) {
            PreparedStatement updateRow = connection.prepareStatement( updateSql );
            bind( updateRow, key, value );
            updateRow.execute();

            connection.close();
        } catch ( SQLException e ) {
            logger.error( "Error executing SQL during store for key {}.", key, e );
        }
    }

    @Override
    public void storeAll( Map<UUID, UUID> map ) {
        UUID key = null;
        try ( Connection connection = hds.getConnection();
                PreparedStatement updateRow = connection.prepareStatement( updateSql ) ) {
            connection.setAutoCommit( false );
            for ( Map.Entry<UUID, UUID> entry : map.entrySet() ) {
                key = entry.getKey();
                bind( updateRow, key, entry.getValue() );
                try {
                    updateRow.addBatch();
                } catch ( SQLException e ) {
                    connection.commit();
                    logger.error( "Unable to store row {} -> {}",
                            key,
                            entry.getValue(), e );
                }
                updateRow.executeBatch();
                updateRow.clearParameters();
            }
            connection.commit();
            connection.setAutoCommit( true );
            connection.close();
        } catch ( SQLException e ) {
            logger.error( "Error executing SQL during store all", e );
        }
    }

    @Override
    public Map<UUID, UUID> loadAll( Collection<UUID> keys ) {
        return keys.parallelStream().distinct().collect( Collectors.toConcurrentMap( Function.identity(), this::load ) );
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public UUID generateTestValue() {
        return UUID.randomUUID();
    }
}
