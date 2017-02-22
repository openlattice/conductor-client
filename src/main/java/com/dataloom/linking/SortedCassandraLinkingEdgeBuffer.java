package com.dataloom.linking;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.util.Util;

/**
 * Manages a sorted buffer of {@link WeightedLinkingEdge} from Cassandra at a local node for processing. This class is
 * not thread safe and shouldn't be accessed by multiple threads without external synchronization.
 * 
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class SortedCassandraLinkingEdgeBuffer {
    private static final Logger                              logger                  = LoggerFactory
            .getLogger( SortedCassandraLinkingEdgeBuffer.class );
    private static final String                              LOWERBOUND              = "lowerbound";
    private static final String                              UPPERBOUND              = "upperbound";
    // private final ConcurrentSkipListSet<WeightedLinkingEdge> buffer = new ConcurrentSkipListSet<>();
    private final PriorityBlockingQueue<WeightedLinkingEdge> buffer;
    private final Session                                    session;
    private final PreparedStatement                          lighestEdge;
    private final PreparedStatement                          addEdge;
    private final PreparedStatement                          getEdge;
    private final PreparedStatement                          removeEdge;
    private final PreparedStatement                          removeEdgeWeight;
    private final PreparedStatement                          addEdgeIfNotExists;
    private final PreparedStatement                          unsafeAddEdge;
    private final UUID                                       graphId;
    private final double                                     threshold;
    private final int                                        bufferReadSize;
    private final int                                        bufferTriggerSize;
    private double                                           heaviestWeightLoadedFromCassandra;
    private final AtomicInteger                              remainingBeforeNextRead = new AtomicInteger();
    private Set<ResultSetFuture>                             pendingOperations;
    private boolean                                          exhausted               = false;
    private final ReentrantLock                              lock                    = new ReentrantLock();

    public SortedCassandraLinkingEdgeBuffer(
            String keyspace,
            Session session,
            UUID graphId,
            double threshold ) {
        this( keyspace, session, graphId, threshold, 4096, 512 );
    }

    public SortedCassandraLinkingEdgeBuffer(
            String keyspace,
            Session session,
            UUID graphId,
            double threshold,
            int bufferReadSize,
            int bufferTriggerSize ) {
        this.session = session;
        this.graphId = graphId;
        this.threshold = threshold;
        this.bufferReadSize = bufferReadSize;
        this.bufferTriggerSize = bufferTriggerSize;
        this.getEdge = session.prepare( getEdgeQuery( keyspace ) );
        this.lighestEdge = session.prepare( lighestEdgeQuery( keyspace ) );
        this.addEdge = session.prepare( Table.WEIGHTED_LINKING_EDGES.getBuilder().buildStoreQuery() );
        this.unsafeAddEdge = session.prepare( Table.LINKING_EDGES.getBuilder().buildStoreQuery() );
        this.addEdgeIfNotExists = session.prepare( Table.LINKING_EDGES.getBuilder().buildStoreQuery().ifNotExists() );
        this.removeEdge = session.prepare( Table.LINKING_EDGES.getBuilder().buildDeleteQuery() );
        this.removeEdgeWeight = session.prepare( Table.WEIGHTED_LINKING_EDGES.getBuilder().buildDeleteQuery() );
        this.pendingOperations = new HashSet<>( Math.max( bufferReadSize, 1 ) );
        this.buffer = new PriorityBlockingQueue<>( Math.max( bufferReadSize, 1 ) );
        replenishBuffer();
    }

    public WeightedLinkingEdge getLightestEdge() {
        replenishBuffer();
        if ( exhausted && buffer.isEmpty() ) {
            return null;
        }

        WeightedLinkingEdge weightedEdge;
        try {
            weightedEdge = buffer.take();
        } catch ( InterruptedException e ) {
            logger.info( "Interrupted while attempting to retrieve lightest edge from buffer of size {}",
                    buffer.size() );
            return null;
        }

        remainingBeforeNextRead.decrementAndGet();
        logger.info( "Retrieved lightest edge from buffer of size {}", buffer.size() );
        return weightedEdge;
    }

    public List<ResultSetFuture> asyncUnsafeAddEdge( WeightedLinkingEdge edge ) {
        return lockAndExecute( () -> {
            List<ResultSetFuture> ops = ImmutableList.of(
                    submitAsyncUnsafeAddEdge( edge ),
                    submitAsyncAddEdgeWeight( edge ) );
            pendingOperations.addAll( ops );
            return ops;
        } );
    }

    public void asyncAddEdgeIfNotExists( WeightedLinkingEdge edge ) {
        lockAndExecute( () -> {
            ResultSetFuture rsf = submitAddEdgeIfNotExists( edge.getEdge() );
            pendingOperations.add( rsf );
            Futures.addCallback( rsf, new FutureCallback<ResultSet>() {
                @Override
                public void onSuccess( ResultSet result ) {
                    if ( Util.wasLightweightTransactionApplied( result ) ) {
                        pendingOperations.add( submitAsyncAddEdgeWeight( edge ) );
                    } else {
                        logger.error( "Failed to add edge {} because it already exists.", edge );
                    }
                }

                @Override
                public void onFailure( Throwable t ) {
                    logger.error( "Unable to execute runnable for edge {}", edge, t );
                }
            } );
        } );
    }

    public boolean tryAddEdge( LinkingEdge edge ) {
        return lockAndExecute( () -> {
            return Util.wasLightweightTransactionApplied( submitAddEdgeIfNotExists( edge ).getUninterruptibly() );
        } );
    }

    public void setEdgeWeight( WeightedLinkingEdge edge ) {
        lockAndExecute( () -> submitAsyncAddEdgeWeight( edge ).getUninterruptibly() );
    }

    public void addEdgeIfNotExists( WeightedLinkingEdge edge ) {
        lockAndExecute( () -> {
            if ( tryAddEdge( edge.getEdge() ) ) {
                setEdgeWeight( edge );
            } else {
                logger.error( "Failed to add edge {} because it already exists.", edge );
            }
        } );
    }

    public List<ResultSetFuture> removeEdge( WeightedLinkingEdge edge ) {
        if ( edge.getWeight() <= heaviestWeightLoadedFromCassandra && buffer.remove( edge ) ) {
            remainingBeforeNextRead.decrementAndGet();
        }

//        if( buffer.contains( edge ) ) {
//            logger.info( "Trying to remove edge that is present." );
//        } else {
//            logger.info( "Trying to remove edge that is not present: {}" , edge );
//        }
//        
        
//        if( buffer.contains( edge ) ) {
//            logger.info( "Edge is still there wtf." );
//        }
        
        return ImmutableList.of(
                submitRemoveEdge( edge.getEdge() ),
                submitRemoveEdgeWeight( edge ) );
    }

    public ResultSetFuture executeIfEdgeNotExists( LinkingEdge edge, Runnable runnable ) {
        ResultSetFuture rsf = session.executeAsync( getEdge.bind()
                .setUUID( CommonColumns.GRAPH_ID.cql(), edge.getGraphId() )
                .setUUID( CommonColumns.SOURCE_LINKING_VERTEX_ID.cql(), edge.getSrc().getVertexId() )
                .setUUID( CommonColumns.DESTINATION_LINKING_VERTEX_ID.cql(), edge.getDst().getVertexId() ) );

        Futures.addCallback( rsf, new FutureCallback<ResultSet>() {

            @Override
            public void onSuccess( ResultSet result ) {
                if ( result.one() == null ) {
                    runnable.run();
                }

            }

            @Override
            public void onFailure( Throwable t ) {
                logger.error( "Unable to execute runnable for edge {}", edge );
            }
        } );
        return rsf;
    }

    public void waitForPendingOperations() {
        logger.info( "Waiting for pending operations to settle." );
        final Set<ResultSetFuture> currentlyPending = pendingOperations;
        pendingOperations = new HashSet<>( bufferReadSize );
        currentlyPending.forEach( ResultSetFuture::getUninterruptibly );
    }

    private <T> T lockAndExecute( Callable<T> runnable ) {
        try {
            lock.lock();
            return runnable.call();
        } catch ( Exception e ) {
            logger.error( "Unable to execute callable.", e );
            return null;
        } finally {
            lock.unlock();
        }
    }

    private void lockAndExecute( Runnable runnable ) {
        try {
            lock.lock();
            runnable.run();
        } finally {
            lock.unlock();
        }
    }

    private ResultSetFuture submitAsyncUnsafeAddEdge( WeightedLinkingEdge edge ) {
        final LinkingEdge le = edge.getEdge();
        return session.executeAsync( unsafeAddEdge
                .bind()
                .setUUID( CommonColumns.GRAPH_ID.cql(), le.getGraphId() )
                .setUUID( CommonColumns.SOURCE_LINKING_VERTEX_ID.cql(), le.getSrc().getVertexId() )
                .setUUID( CommonColumns.DESTINATION_LINKING_VERTEX_ID.cql(), le.getDst().getVertexId() ) );
    }

    private static class Counter {
        int count = 0;
    }

    private void replenishBuffer() {
        if ( ( remainingBeforeNextRead.get() <= bufferTriggerSize ) && !exhausted ) {
            try {
                lock.lock();
                waitForPendingOperations();
                logger.info( "Refilling buffer from Cassandra" );
                final Counter counter = new Counter();
                lightestEdges()
                        .peek( e -> {
                            heaviestWeightLoadedFromCassandra = Math.max( heaviestWeightLoadedFromCassandra,
                                    e.getWeight() );
                            counter.count++;
                        } )
                        .forEach( buffer::add );
                remainingBeforeNextRead.getAndUpdate( remaining -> remaining + counter.count );
                exhausted = ( !lock.hasQueuedThreads() && counter.count == 0 );
            } finally {
                lock.unlock();
            }
        }
    }

    private Stream<WeightedLinkingEdge> lightestEdges() {
        ResultSet rs = session.execute( lighestEdge
                .bind()
                .setUUID( CommonColumns.GRAPH_ID.cql(), graphId )
                .setDouble( LOWERBOUND, heaviestWeightLoadedFromCassandra )
                .setDouble( UPPERBOUND, threshold ) );
        return StreamUtil
                .stream( rs )
                .map( LinkingUtil::weightedEdge );
    }

    private Select lighestEdgeQuery( String keyspace ) {
        return QueryBuilder
                .select( CommonColumns.GRAPH_ID.cql(),
                        CommonColumns.SOURCE_LINKING_VERTEX_ID.cql(),
                        CommonColumns.DESTINATION_LINKING_VERTEX_ID.cql(),
                        CommonColumns.EDGE_VALUE.cql() )
                .from( keyspace, Table.WEIGHTED_LINKING_EDGES.getName() )
                .where( CommonColumns.GRAPH_ID.eq() )
                .and( QueryBuilder.gte( CommonColumns.EDGE_VALUE.cql(), QueryBuilder.bindMarker( LOWERBOUND ) ) )
                .and( QueryBuilder.lt( CommonColumns.EDGE_VALUE.cql(), QueryBuilder.bindMarker( UPPERBOUND ) ) )
                .limit( Math.max( bufferReadSize, 1 ) );
    }

    private ResultSetFuture submitAddEdgeIfNotExists( LinkingEdge edge ) {
        return session.executeAsync( addEdgeIfNotExists
                .bind()
                .setUUID( CommonColumns.GRAPH_ID.cql(), edge.getGraphId() )
                .setUUID( CommonColumns.SOURCE_LINKING_VERTEX_ID.cql(), edge.getSrc().getVertexId() )
                .setUUID( CommonColumns.DESTINATION_LINKING_VERTEX_ID.cql(), edge.getDst().getVertexId() ) );
    }

    private ResultSetFuture submitRemoveEdge( LinkingEdge edge ) {
        return session.executeAsync( removeEdge
                .bind()
                .setUUID( CommonColumns.GRAPH_ID.cql(), edge.getGraphId() )
                .setUUID( CommonColumns.SOURCE_LINKING_VERTEX_ID.cql(), edge.getSrc().getVertexId() )
                .setUUID( CommonColumns.DESTINATION_LINKING_VERTEX_ID.cql(), edge.getDst().getVertexId() ) );
    }

    private ResultSetFuture submitRemoveEdgeWeight( WeightedLinkingEdge edge ) {
        final LinkingEdge le = edge.getEdge();
        return session.executeAsync( removeEdgeWeight
                .bind()
                .setUUID( CommonColumns.GRAPH_ID.cql(), le.getGraphId() )
                .setUUID( CommonColumns.SOURCE_LINKING_VERTEX_ID.cql(), le.getSrc().getVertexId() )
                .setUUID( CommonColumns.DESTINATION_LINKING_VERTEX_ID.cql(), le.getDst().getVertexId() )
                .setDouble( CommonColumns.EDGE_VALUE.cql(), edge.getWeight() ) );
    }

    private ResultSetFuture submitAsyncAddEdgeWeight( WeightedLinkingEdge edge ) {
        /*
         * Since we already know everything less than the heaviest weight that's been loaded from Cassandra we can
         * safely add to the buffer and increment the number before next read.
         */
        if ( edge.getWeight() <= heaviestWeightLoadedFromCassandra ) {
            remainingBeforeNextRead.incrementAndGet();
            buffer.add( edge );
        }

        final LinkingEdge le = edge.getEdge();
        return session.executeAsync( addEdge
                .bind()
                .setUUID( CommonColumns.GRAPH_ID.cql(), le.getGraphId() )
                .setUUID( CommonColumns.SOURCE_LINKING_VERTEX_ID.cql(), le.getSrc().getVertexId() )
                .setUUID( CommonColumns.DESTINATION_LINKING_VERTEX_ID.cql(), le.getDst().getVertexId() )
                .setDouble( CommonColumns.EDGE_VALUE.cql(), edge.getWeight() ) );
    }

    private static Select.Where getEdgeQuery( String keyspace ) {
        return QueryBuilder.select( CommonColumns.GRAPH_ID.cql(),
                CommonColumns.SOURCE_LINKING_VERTEX_ID.cql(),
                CommonColumns.DESTINATION_LINKING_VERTEX_ID.cql(),
                CommonColumns.EDGE_VALUE.cql() )
                .from( Table.WEIGHTED_LINKING_EDGES.getKeyspace(), Table.WEIGHTED_LINKING_EDGES.getName() )
                .allowFiltering()
                .where( CommonColumns.GRAPH_ID.eq() )
                .and( CommonColumns.SOURCE_LINKING_VERTEX_ID.eq() )
                .and( CommonColumns.DESTINATION_LINKING_VERTEX_ID.eq() );

    }

}
