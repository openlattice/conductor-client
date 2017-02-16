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

import com.dataloom.data.EntityKey;
import com.dataloom.graph.DirectedEdge;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Select;

import java.util.Map;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class CassandraLinkingGraphsQueryService {
    private final Session session;
    public CassandraLinkingGraphsQueryService( String keyspace, Session session ) {
        this.session = session;
    }

    public Map<UUID,Double> getNeighbors( LinkingEdge vertices ) {
        return null;
    }

    public Iterable<DirectedEdge> getOutgoingEdges( EntityKey vertex ) {
        return null;
    }


    public static Select.Where neighborsQuery(String keyspace ) {
        return null;
    }

    public WeightedLinkingEdge getLightestEdge( UUID graphId ) {
        return null;
    }

    public Map<UUID,Double> getDstNeighbors( LinkingEdge edge ) {
        return getNeighbors( edge.getDst(), edge.getSrc() );
    }

    public Map<UUID,Double> getSrcNeighbors( LinkingEdge edge ) {
        return getNeighbors( edge.getSrc(), edge.getDst() );
    }

    public Map<UUID, Double> getNeighbors( LinkingVertexKey center, LinkingVertexKey exclude ) {
        return null;
    }
}
