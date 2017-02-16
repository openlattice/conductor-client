package com.dataloom.graph;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.mapstores.LinkingVertexKey;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.SortedSetMultimap;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;
import java.util.UUID;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class CassandraGraphQueryService {
    private final Session session;
    public CassandraGraphQueryService( String keyspace, Session session ) {
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

    public Pair<LinkingEdge,Double> getLightestEdge( UUID graphId ) {
        return null;
    }

    public  getNeighbors( LinkingEdge key ) {
    }


    public Map<UUID,Double> getDstNeighbors( LinkingEdge edge ) {
        return getNeighbors( edge.getDst(), edge.getSrc() );
    }

    public Map<UUID,Double> getSrcNeighbors( LinkingEdge edge ) {
        return getNeighbors( edge.getSrc(), edge.getDst() );
    }

    public Map<UUID, Double> getNeighbors( LinkingVertexKey center, LinkingVertexKey exclude ) {

    }
}
