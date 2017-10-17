package com.openlattice.postgres.mapstores;

import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.linking.LinkingVertex;
import com.dataloom.linking.LinkingVertexKey;
import com.dataloom.linking.WeightedLinkingVertexKeySet;
import com.google.common.collect.ImmutableList;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang.NotImplementedException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.openlattice.postgres.PostgresColumn.*;
import static com.openlattice.postgres.PostgresTable.LINKING_EDGES;

public class LinkingEdgesMapstore extends AbstractBasePostgresMapstore<LinkingVertexKey, WeightedLinkingVertexKeySet> {
    public LinkingEdgesMapstore( HikariDataSource hds ) {
        super( HazelcastMap.LINKING_EDGES.name(), LINKING_EDGES, hds );
    }

    @Override protected List<PostgresColumnDefinition> keyColumns() {
        return ImmutableList.of( GRAPH_ID, SRC_LINKING_VERTEX_ID );
    }

    @Override protected List<PostgresColumnDefinition> valueColumns() {
        return ImmutableList.of( EDGE_VALUE, DST_LINKING_VERTEX_ID );
    }

    @Override protected void bind(
            PreparedStatement ps, LinkingVertexKey key, WeightedLinkingVertexKeySet value ) throws SQLException {
        throw new NotImplementedException( "Something went very wrong. This should never happen." );
    }

    @Override protected void bind( PreparedStatement ps, LinkingVertexKey key ) throws SQLException {
        throw new NotImplementedException( "Something went very wrong. This should never happen." );
    }

    @Override protected WeightedLinkingVertexKeySet mapToValue( ResultSet rs ) throws SQLException {
        throw new NotImplementedException( "Something went very wrong. This should never happen." );
    }

    @Override protected LinkingVertexKey mapToKey( ResultSet rs ) {
        throw new NotImplementedException( "Something went very wrong. This should never happen." );
    }

    @Override public MapConfig getMapConfig() {
        return super.getMapConfig()
                .addMapIndexConfig( new MapIndexConfig( "__key#graphId", false ) )
                .addMapIndexConfig( new MapIndexConfig( "__key#vertexId", false ) )
                .addMapIndexConfig( new MapIndexConfig( "value[any].vertexKey", false ) );
    }

    @Override public MapStoreConfig getMapStoreConfig() {
        return super.getMapStoreConfig()
                .setEnabled( false );
    }

    @Override
    public void delete( LinkingVertexKey key ) {
        throw new NotImplementedException( "Something went very wrong. This should never happen." );
    }

    @Override
    public void deleteAll( Collection<LinkingVertexKey> keys ) {
        throw new NotImplementedException( "Something went very wrong. This should never happen." );
    }

    @Override
    public void store( LinkingVertexKey key, WeightedLinkingVertexKeySet value ) {
        throw new NotImplementedException( "Something went very wrong. This should never happen." );
    }

    @Override
    public void storeAll( Map<LinkingVertexKey, WeightedLinkingVertexKeySet> entries ) {
        throw new NotImplementedException( "Something went very wrong. This should never happen." );
    }

    @Override
    public WeightedLinkingVertexKeySet load( LinkingVertexKey key ) {
        throw new NotImplementedException( "Something went very wrong. This should never happen." );
    }

    @Override
    public Map<LinkingVertexKey, WeightedLinkingVertexKeySet> loadAll( Collection<LinkingVertexKey> keys ) {
        throw new NotImplementedException( "Something went very wrong. This should never happen." );
    }

    @Override public LinkingVertexKey generateTestKey() {
        return new LinkingVertexKey( UUID.randomUUID(), UUID.randomUUID() );
    }

    @Override public WeightedLinkingVertexKeySet generateTestValue() {
        return new WeightedLinkingVertexKeySet();
    }
}
