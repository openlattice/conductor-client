package com.dataloom.linking;

import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;

/**
 * Manages a sorted buffer of {@link WeightedLinkingEdge} from Cassandra at a local node for processing.
 * 
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class SortedCassandraLinkingEdgeBuffer {
    private static final Logger                              logger               = LoggerFactory
            .getLogger( SortedCassandraLinkingEdgeBuffer.class );
    private static final int                                 BUFFER_REFRESH_SIZE  = 4096;
    private static final int                                 BUFFER_REFRESH_BOUND = 512;
    private static final String                              LOWERBOUND           = "lowerbound";
    private static final String                              UPPERBOUND           = "upperbound";
    private final ConcurrentSkipListSet<WeightedLinkingEdge> buffer               = new ConcurrentSkipListSet<>();
    private final Session                                    session;
    private final PreparedStatement                          lighestEdge;
    private final HazelcastLinkingGraphs                     linkingGraphs;
    private final UUID                                       graphId;
    private final double                                     threshold;
    private double                                           lowerbound           = 0.0D;
    private double                                           heaviestWeightLoadedFromCassandra;

    public SortedCassandraLinkingEdgeBuffer(
            String keyspace,
            Session session,
            HazelcastLinkingGraphs linkingGraphs,
            UUID graphId,
            double threshold ) {
        this.session = session;
        this.linkingGraphs = linkingGraphs;
        this.graphId = graphId;
        this.threshold = threshold;
        this.lighestEdge = session.prepare( lighestEdgeQuery( keyspace ) );
        replenishBuffer();
    }

    public WeightedLinkingEdge getLightestEdge() {
        WeightedLinkingEdge weightedEdge = buffer.pollFirst();

        if ( weightedEdge == null ) {
            replenishBuffer();
            if ( buffer.size() == 0 ) {
                return null;
            }
            return getLightestEdge();
        }

        if ( weightedEdge.getWeight() > heaviestWeightLoadedFromCassandra ) {
            buffer.clear();
            replenishBuffer();
            if ( buffer.size() == 0 ) {
                return null;
            }
            return getLightestEdge();
        }

        lowerbound = weightedEdge.getWeight();
        return weightedEdge;
    }

    public void addEdge( WeightedLinkingEdge edge ) {
        buffer.add( edge );
        linkingGraphs.addEdge( edge.getEdge(), edge.getWeight() );
    }

    public void removeEdge( LinkingEdge edge ) {
        Double weight = linkingGraphs.getWeight( edge );
        if ( weight == null ) {
            logger.info( "Edge has already been deleted. Encountered null weight for edge: {}" );
        } else {
            removeEdge( new WeightedLinkingEdge( weight.doubleValue(), edge ) );
        }
    }

    public void removeEdge( WeightedLinkingEdge edge ) {
        buffer.remove( edge );
        linkingGraphs.removeEdge( edge.getEdge() );
        replenishBuffer();
    }

    private synchronized void replenishBuffer() {
        if ( buffer.size() < BUFFER_REFRESH_BOUND ) {
            lightestEdges( lowerbound )
                    .peek( e -> {
                        heaviestWeightLoadedFromCassandra = Math.max( heaviestWeightLoadedFromCassandra,
                                e.getWeight() );
                    } )
                    .forEach( buffer::add );
        }
    }

    private Stream<WeightedLinkingEdge> lightestEdges( double lowerbound ) {
        ResultSet rs = session.execute( lighestEdge
                .bind()
                .setUUID( CommonColumns.GRAPH_ID.cql(), graphId )
                .setDouble( LOWERBOUND, lowerbound )
                .setDouble( UPPERBOUND, threshold ) );
        return StreamUtil
                .stream( rs )
                .map( LinkingUtil::weightedEdge );
    }

    public static Select lighestEdgeQuery( String keyspace ) {
        return QueryBuilder
                .select( CommonColumns.GRAPH_ID.cql(),
                        CommonColumns.SOURCE_LINKING_VERTEX_ID.cql(),
                        CommonColumns.DESTINATION_LINKING_VERTEX_ID.cql(),
                        CommonColumns.EDGE_VALUE.cql() )
                .from( keyspace, Table.LINKING_EDGES.getName() )
                .where( CommonColumns.GRAPH_ID.eq() )
                .and( QueryBuilder.gt( CommonColumns.EDGE_VALUE.cql(), QueryBuilder.bindMarker( LOWERBOUND ) ) )
                .and( QueryBuilder.lt( CommonColumns.EDGE_VALUE.cql(), QueryBuilder.bindMarker( UPPERBOUND ) ) )
                .limit( BUFFER_REFRESH_SIZE );
    }

}
