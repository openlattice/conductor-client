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

package com.dataloom.graph.mapstores;

import java.util.UUID;

import org.apache.commons.lang.math.RandomUtils;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.GraphUtil;
import com.dataloom.graph.LinkingEdge;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class LinkingGraphsMapstore extends AbstractStructuredCassandraMapstore<LinkingEdge, Double> {
    public LinkingGraphsMapstore( Session session ) {
        super( HazelcastMap.LINKING_EDGES.name(), session, Table.LINKING_EDGES.getBuilder() );
    }

    @Override
    protected BoundStatement bind( LinkingEdge key, BoundStatement bs ) {
        final EntityKey src = key.getSource();
        final EntityKey dst = key.getDestination();
        return bs.setUUID( CommonColumns.GRAPH_ID.cql(), key.getGraphId() )
                .setUUID( CommonColumns.SOURCE_ENTITY_SET_ID.cql(), src.getEntitySetId() )
                .setString( CommonColumns.SOURCE_ENTITY_ID.cql(), src.getEntityId() )
                .setUUID( CommonColumns.DESTINATION_ENTITY_SET_ID.cql(), dst.getEntitySetId() )
                .setString( CommonColumns.DESTINATION_ENTITY_ID.cql(), dst.getEntityId() );
    }

    @Override
    protected BoundStatement bind( LinkingEdge key, Double value, BoundStatement bs ) {
        return bind( key, bs ).setDouble( CommonColumns.EDGE_VALUE.cql(), value );

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

    public static LinkingEdge testKey() {
        return new LinkingEdge( UUID.randomUUID(), TestDataFactory.entityKey(), TestDataFactory.entityKey() );
    }
}
