package com.dataloom.graph;

import com.dataloom.data.EntityKey;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Select;


/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class CassandraGraphQueryService {
    private final Session session;
    public CassandraGraphQueryService( String keyspace, Session session ) {
        this.session = session;
    }

    public Iterable<EntityKey> getNeighbors( EntityKey vertex ) {

        return null;
    }

    public Iterable<DirectedEdge> getOutgoingEdges( EntityKey vertex ) {
        return null;
    }

    public static Select.Where neighborsQuery(String keyspace ) {
        return null;
    }
}
