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

import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;

import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class CassandraLinkingGraphsQueryService {
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
                .from( keyspace, Table.LINKING_EDGES.getName() )
                .where( CommonColumns.GRAPH_ID.eq() )
                .limit( 1 );
    }

    public static Select.Where srcNeighborsQuery( String keyspace ) {
        return QueryBuilder
                .select( CommonColumns.DESTINATION_LINKING_VERTEX_ID.cql(), CommonColumns.EDGE_VALUE.cql() )
                .from( keyspace, Table.LINKING_EDGES.getName() )
                .where( CommonColumns.GRAPH_ID.eq() ).and( CommonColumns.SOURCE_LINKING_VERTEX_ID.eq() );
    }

    public static Select.Where dstNeighborsQuery( String keyspace ) {
        return QueryBuilder
                .select( CommonColumns.SOURCE_LINKING_VERTEX_ID.cql(), CommonColumns.EDGE_VALUE.cql() )
                .from( keyspace, Table.LINKING_EDGES.getName() )
                .where( CommonColumns.GRAPH_ID.eq() ).and( CommonColumns.DESTINATION_LINKING_VERTEX_ID.eq() );
    }

    public WeightedLinkingEdge getLightestEdge( UUID graphId ) {
        ResultSet rs = session.execute( lighestEdge.bind().setUUID( CommonColumns.GRAPH_ID.cql(), graphId ) );
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        LinkingEdge edge = LinkingUtil.linkingEdge( row );
        Double weight = LinkingUtil.edgeValue( row );
        return new WeightedLinkingEdge( weight.doubleValue(), edge );
    }

    public Map<UUID, Double> getDstNeighbors( LinkingEdge edge ) {
        BoundStatement bs = dstNeighbors.bind()
                .setUUID( CommonColumns.GRAPH_ID.cql(), edge.getGraphId() )
                .setUUID( CommonColumns.DESTINATION_LINKING_VERTEX_ID.cql(), edge.getDst().getVertexId() );

        Map<UUID,Double> neighbors = StreamUtil.stream( session.execute( bs ) )
                .collect( Collectors.toMap( LinkingUtil::srcId, LinkingUtil::edgeValue ) );

        neighbors.remove( edge.getSrc() );
        return neighbors;
    }

    public Map<UUID, Double> getSrcNeighbors( LinkingEdge edge ) {
        BoundStatement bs = srcNeighbors.bind()
                .setUUID( CommonColumns.GRAPH_ID.cql(), edge.getGraphId() )
                .setUUID( CommonColumns.SOURCE_LINKING_VERTEX_ID.cql(), edge.getSrc().getVertexId() );

        Map<UUID,Double> neighbors =  StreamUtil.stream( session.execute( bs ) )
                .collect( Collectors.toMap( LinkingUtil::dstId, LinkingUtil::edgeValue ) );

        neighbors.remove( edge.getDst() );
        return neighbors;
    }
}
