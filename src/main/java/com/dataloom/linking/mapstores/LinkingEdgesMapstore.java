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

package com.dataloom.linking.mapstores;

import com.dataloom.graph.GraphUtil;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.linking.LinkingEdge;
import com.dataloom.linking.LinkingVertexKey;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;
import org.apache.commons.lang.math.RandomUtils;

import java.util.Collection;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class LinkingEdgesMapstore extends AbstractStructuredCassandraMapstore<LinkingEdge, Double> {
    public LinkingEdgesMapstore( Session session ) {
        super( HazelcastMap.LINKING_EDGES.name(), session, Table.LINKING_EDGES.getBuilder() );
    }

    public static LinkingEdge testKey() {
        return new LinkingEdge( new LinkingVertexKey( UUID.randomUUID(), UUID.randomUUID() ),
                new LinkingVertexKey( UUID.randomUUID(), UUID.randomUUID() ) );
    }

    @Override
    protected BoundStatement bind( LinkingEdge key, BoundStatement bs ) {
        final LinkingVertexKey src = key.getSrc();
        final LinkingVertexKey dst = key.getDst();
        return bs.setUUID( CommonColumns.GRAPH_ID.cql(), key.getGraphId() )
                .setUUID( CommonColumns.SOURCE_LINKING_VERTEX_ID.cql(), src.getVertexId() )
                .setUUID( CommonColumns.DESTINATION_LINKING_VERTEX_ID.cql(), dst.getVertexId() );
    }

    @Override
    protected BoundStatement bind( LinkingEdge key, Double value, BoundStatement bs ) {
        return bind( key, bs ).setDouble( CommonColumns.EDGE_VALUE.cql(), value );

    }

    @Override protected Select.Where loadQuery() {
        return QueryBuilder.select( CommonColumns.GRAPH_ID.cql(),
                CommonColumns.SOURCE_LINKING_VERTEX_ID.cql(),
                CommonColumns.DESTINATION_LINKING_VERTEX_ID.cql(),
                CommonColumns.EDGE_VALUE.cql() )
                .from( Table.LINKING_EDGES.getKeyspace(), Table.LINKING_EDGES.getName() )
                .allowFiltering()
                .where( CommonColumns.GRAPH_ID.eq() )
                .and( CommonColumns.SOURCE_LINKING_VERTEX_ID.eq() )
                .and( CommonColumns.DESTINATION_LINKING_VERTEX_ID.eq() );
    }

    //    @Override protected RegularStatement deleteQuery() {
    //        return QueryBuilder.delete()
    //                .from( Table.LINKING_EDGES.getKeyspace(), Table.LINKING_EDGES.getName() )
    //                .where( QueryBuilder.gt( CommonColumns.EDGE_VALUE.cql(), CommonColumns.EDGE_VALUE.bindMarker() ) )
    //                .and( CommonColumns.GRAPH_ID.eq() )
    //                .and( CommonColumns.SOURCE_LINKING_VERTEX_ID.eq() )
    //                .and( CommonColumns.DESTINATION_LINKING_VERTEX_ID.eq() );
    //    }

    @Override public void delete( LinkingEdge key ) {
        session.execute( bind( key, load( key ), getDeleteQuery().bind() ) );
    }

    @Override
    public void deleteAll( Collection<LinkingEdge> keys ) {
        keys.forEach( this::delete );
    }

    @Override
    protected LinkingEdge mapKey( Row row ) {
        return GraphUtil.linkingEdge( row );
    }

    @Override
    protected Double mapValue( ResultSet rs ) {
        Row row = rs.one();
        return row == null ? null : GraphUtil.edgeValue( row );
    }

    @Override
    public LinkingEdge generateTestKey() {
        return testKey();
    }

    @Override
    public Double generateTestValue() {
        return RandomUtils.nextDouble();
    }
}
