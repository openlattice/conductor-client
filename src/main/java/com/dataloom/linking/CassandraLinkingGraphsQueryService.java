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

package com.dataloom.linking;

import java.util.Map;
import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.base.Preconditions;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;

import jersey.repackaged.com.google.common.collect.Maps;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class CassandraLinkingGraphsQueryService {
    private static final String     LOWERBOUND = "lowerbound";
    private static final String     UPPERBOUND = "upperbound";
    private final Session           session;
    private final PreparedStatement srcNeighbors;
    private final PreparedStatement dstNeighbors;
    private final PreparedStatement lighestEdge;

    public CassandraLinkingGraphsQueryService( String keyspace, Session session ) {
        this.session = session;
        this.srcNeighbors = session.prepare( srcNeighborsQuery( keyspace ) );
        this.dstNeighbors = session.prepare( dstNeighborsQuery( keyspace ) );
        this.lighestEdge = session.prepare( lighestEdgeQuery( keyspace ) );
    }

    public static Select lighestEdgeQuery( String keyspace ) {
        return QueryBuilder
                .select( CommonColumns.GRAPH_ID.cql(),
                        CommonColumns.SOURCE_LINKING_VERTEX_ID.cql(),
                        CommonColumns.DESTINATION_LINKING_VERTEX_ID.cql(),
                        CommonColumns.EDGE_VALUE.cql() )
                .from( keyspace, Table.WEIGHTED_LINKING_EDGES.getName() )
                .where( CommonColumns.GRAPH_ID.eq() )
                .and( QueryBuilder.gt( CommonColumns.EDGE_VALUE.cql(), QueryBuilder.bindMarker( LOWERBOUND ) ) )
                .and( QueryBuilder.lt( CommonColumns.EDGE_VALUE.cql(), QueryBuilder.bindMarker( UPPERBOUND ) ) )
                .limit( 1 );
    }

    public static Select.Where srcNeighborsQuery( String keyspace ) {
        return QueryBuilder
                .select( CommonColumns.DESTINATION_LINKING_VERTEX_ID.cql(), CommonColumns.EDGE_VALUE.cql() )
                .from( keyspace, Table.WEIGHTED_LINKING_EDGES.getName() )
                .where( CommonColumns.GRAPH_ID.eq() ).and( CommonColumns.SOURCE_LINKING_VERTEX_ID.eq() );
    }

    public static Select.Where dstNeighborsQuery( String keyspace ) {
        return QueryBuilder
                .select( CommonColumns.SOURCE_LINKING_VERTEX_ID.cql(), CommonColumns.EDGE_VALUE.cql() )
                .from( keyspace, Table.WEIGHTED_LINKING_EDGES.getName() )
                .where( CommonColumns.GRAPH_ID.eq() ).and( CommonColumns.DESTINATION_LINKING_VERTEX_ID.eq() );
    }

    public WeightedLinkingEdge getLightestEdge( UUID graphId, double lowerbound, double upperbound ) {
        ResultSet rs = session.execute( lighestEdge
                .bind()
                .setUUID( CommonColumns.GRAPH_ID.cql(), graphId )
                .setDouble( LOWERBOUND, lowerbound )
                .setDouble( UPPERBOUND, upperbound ) );

        Row row = rs.one();
        if ( row == null ) {
            return null;
        }

        LinkingEdge edge = LinkingUtil.linkingEdge( row );
        Double weight = LinkingUtil.edgeValue( row );
        return new WeightedLinkingEdge( weight.doubleValue(), edge );
    }

    public Map<UUID, Double> getDstNeighbors( LinkingEdge edge ) {
        ResultSet srcNeighbors = executeSrcNeighbors( edge.getDst() ); // All neighbors were getSrc appears Src col
        ResultSet dstNeighbors = executeDstNeighbors( edge.getDst() ); // All neighbors were getSrc appears in Dst col

        Map<UUID, Double> neighbors = Maps.newHashMap();

        for ( Row row : srcNeighbors ) {
            UUID id = LinkingUtil.dstId( row );
            Preconditions.checkState( neighbors.put( id, LinkingUtil.edgeValue( row ) ) == null,
                    "Duplicate Key: %s",
                    id );
        }

        for ( Row row : dstNeighbors ) {
            UUID id = LinkingUtil.srcId( row );
            Preconditions.checkState( neighbors.put( id, LinkingUtil.edgeValue( row ) ) == null,
                    "Duplicate Key: %s",
                    id );
        }

        // Remove the edge represented by edge from results.
        neighbors.remove( edge.getSrc().getVertexId() );

        return neighbors;
    }

    public Map<UUID, Double> getSrcNeighbors( LinkingEdge edge ) {
        ResultSet srcNeighbors = executeSrcNeighbors( edge.getSrc() ); // All neighbors were getSrc appears Src col
        ResultSet dstNeighbors = executeDstNeighbors( edge.getSrc() ); // All neighbors were getSrc appears in Dst col

        Map<UUID, Double> neighbors = Maps.newHashMap();

        for ( Row row : srcNeighbors ) {
            UUID id = LinkingUtil.dstId( row );
            Preconditions.checkState( neighbors.put( id, LinkingUtil.edgeValue( row ) ) == null,
                    "Duplicate Key: %s",
                    id );
        }

        for ( Row row : dstNeighbors ) {
            UUID id = LinkingUtil.srcId( row );
            Preconditions.checkState( neighbors.put( id, LinkingUtil.edgeValue( row ) ) == null,
                    "Duplicate Key: %s",
                    id );
        }

        // Remove the edge represented by edge from results.
        neighbors.remove( edge.getDst().getVertexId() );

        return neighbors;
    }

    private ResultSet executeSrcNeighbors( LinkingVertexKey key ) {
        final UUID graphId = key.getGraphId();
        final UUID vertexId = key.getVertexId();
        BoundStatement bs = srcNeighbors.bind()
                .setUUID( CommonColumns.GRAPH_ID.cql(), graphId )
                .setUUID( CommonColumns.SOURCE_LINKING_VERTEX_ID.cql(), vertexId );
        return session.execute( bs );
    }

    private ResultSet executeDstNeighbors( LinkingVertexKey key ) {
        final UUID graphId = key.getGraphId();
        final UUID vertexId = key.getVertexId();
        BoundStatement dstbs = dstNeighbors.bind()
                .setUUID( CommonColumns.GRAPH_ID.cql(), graphId )
                .setUUID( CommonColumns.DESTINATION_LINKING_VERTEX_ID.cql(), vertexId );

        return session.execute( dstbs );
    }
}
