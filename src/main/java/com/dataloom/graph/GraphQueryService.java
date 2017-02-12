package com.dataloom.graph;

import com.dataloom.data.EntityKey;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.kryptnostic.datastore.cassandra.CommonColumns;

import javax.management.Query;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class CassandraGraphQueryService {
    private final Session session;
    private final PreparedStatement getNeighbors;
    public CassandraGraphQueryService( String keyspace, Session session ) {
        this.session = session;
        session.prepare(  )
    }

    public Iterable<EntityKey> getNeighbors( EntityKey vertex ) {

        return null;
    }

    public Iterable<DirectedEdge> getOutgoingEdges( EntityKey vertex ) {
        return null;
    }

    public static Select.Where neighborsQuery(String keyspace ) {
        return QueryBuilder.select( CommonColumns.DESTINATION )
    }
}
