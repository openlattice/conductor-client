package com.dataloom.graph.core.objects;

import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.core.GraphQueryService;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.datastore.util.Util;

/**
 * LoomVertexFuture is initialized by the entity key, and would fire off async requests to obtain vertex id when
 * initialized.
 * 
 * Vertex Id acquisition is an get-or-create operation. In other words, if there is already a vertex id associated to
 * the entity key, that vertex id would be returned.
 * 
 * When {@link #get()} is called, the thread is blocked until a LoomVertex object is returned. It is possible, although
 * unlikely, that null is returned, indicating a failure in obtaining a vertex id.
 * 
 * The mechanism of getting a vertex id is as follows:
 * <ol>
 * <li>Try a cassandra insert if not exists (entity_key, uuid) into <i>vertices_lookup</i> table:
 * <ul>
 * <li>If the transaction suceeds, proceed to 2.</li>
 * <li>If the lightweight transaction fails, return the vertex with the id that comes in the result set.</li>
 * </ul>
 * </li>
 * <li>Try a cassandra insert if not exists (uuid, entity_key) into <i>vertices</i> table;
 * <ul>
 * <li>If the transaction succeeds, a <i>LoomVertex</i> object with the pair (entity_key, uuid) should be returned.</li>
 * <li>If the transaction fails, repeat step 1 with a new UUID and try again. Here <i>insert if not exists</i> should
 * be changed to update id if entity_key equals (the one we are specifying)</li>
 * </ul>
 * </li>
 * </ol>
 * 
 * @author Ho Chung Siu
 *
 */
public class LoomVertexFuture {
    private static final Logger      logger          = LoggerFactory.getLogger( LoomVertexFuture.class );
    private static GraphQueryService gqs;
    // use designated threadpool to handle async calls
    private static final Executor    executor;
    // maximum number of retrying uuid's
    private static final int         MAX_ID_RETRIES  = 3;

    private EntityKey                reference;
    private UUID                     id;
    private int                      idRetries       = 0;

    private ResultSetFuture          vertexLookupRsf;
    private ResultSetFuture          vertexRsf;

    private boolean                  putVertexLookup = false;
    private boolean                  isDone          = false;

    static {
        ThreadPoolExecutor tp = new ThreadPoolExecutor(
                5,
                5,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>() );
        tp.allowCoreThreadTimeOut( true );
        executor = tp;
    }

    public static void setGraphQueryService( GraphQueryService gqs ) {
        LoomVertexFuture.gqs = gqs;
    }

    public LoomVertexFuture( EntityKey reference ) {
        this.reference = reference;
        this.id = UUID.randomUUID();
        putVertexLookupIfAbsentAsync(); // start stage 1 (async): put into vertex lookup table.
    }

    private void putVertexLookupIfAbsentAsync() {
        vertexLookupRsf = gqs.putVertexLookUpIfAbsentAsync( id, reference );
        addCallbackToVertexLookupAsync();
    }

    private void putVertexIfAbsentAsync() {
        vertexRsf = gqs.putVertexIfAbsentAsync( id, reference );
        addCallbackToVertexAsync();
    }

    private void addCallbackToVertexLookupAsync() {
        Futures.addCallback( vertexLookupRsf, new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess( ResultSet rs ) {
                if ( Util.wasLightweightTransactionApplied( rs ) ) {
                    // stage 1 (put into vertex lookup table) succeeds, callback should execute stage 2 (async): put
                    // into vertex table
                    putVertexLookup = true;
                    putVertexIfAbsentAsync();
                } else {
                    // stage 1 (put into vertex lookup table) fails, because uuid is already associated with the entity
                    // key. The existing id should be returned when Future.get() is called.
                    id = RowAdapters.vertexId( rs.one() );
                    isDone = true;
                }
            }

            @Override
            public void onFailure( Throwable t ) {
                // query for stage 1 (put into vertex lookup table) failed. Would not retry again until Future.get() is
                // called.
                putVertexLookup = false;
            }
        }, executor );
    }

    private void addCallbackToVertexAsync() {
        Futures.addCallback( vertexRsf, new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess( ResultSet rs ) {
                if ( Util.wasLightweightTransactionApplied( rs ) ) {
                    // stage 2 (put into vertex table) succeeds. The chosen id should be returned when Future.get() is
                    // called. Registered listener should be executed.
                    isDone = true;
                } // otherwise stage 2 (put into vertex table) fails, because the chosen uuid is already occupied. This
                  // is a rare case, and we would not retry again until Future.get() is called.
            }

            @Override
            public void onFailure( Throwable t ) {
                // query for stage 2 (put into vertex table) failed. We would just roll back and retry another uuid.
                // Would not retry again until Future.get() is called.
            }
        }, executor );
    }

    public LoomVertex get() {
        try {
            if ( !isDone ) {
                // finish up any unfinished tries.
                if ( putVertexLookup ) {
                    // Stage 1 (put into vertex lookup table) is completed. Get the result for stage 2 (put into vertex
                    // table) and proceed.
                    putVertexIfAbsentResult( vertexRsf.getUninterruptibly() );
                } else {
                    if ( !vertexRsf.isDone() ) {
                        // Stage 1 is (put into vertex lookup table) is not completed yet; get the result and proceed.
                        putVertexLookupIfAbsentResult( vertexRsf.getUninterruptibly() );
                    } else {
                        // query for stage 1 (put into vertex lookup table) failed. Retry stage 1.
                        putVertexLookupIfAbsent();
                    }
                }
            }
            return new LoomVertex( id, reference );
        } catch ( Exception e ) {
            logger.debug( "Getting LoomVertex with id {} failed because: {}.",
                    id,
                    e.getLocalizedMessage() );
            return null;
        }
    }

    /*
     * Process stage 1 (put into vertex lookup table)
     */
    private void putVertexLookupIfAbsent() {
        ResultSet rs = gqs.putVertexLookUpIfAbsentAsync( id, reference ).getUninterruptibly();
        putVertexLookupIfAbsentResult( rs );
    }

    /*
     * Handle what happens after stage 1 (put into vertex lookup table) or stage 3 (update id of vertex lookup table) is
     * finished.
     */
    private void putVertexLookupIfAbsentResult( ResultSet rs ) throws IllegalStateException {
        if ( ++idRetries >= MAX_ID_RETRIES ) {
            throw new IllegalStateException( "Failed to register for vertex id because maximum number of retries is already reached." );
        }
        if ( Util.wasLightweightTransactionApplied( rs ) ) {
            // proceed to put Vertex table
            putVertexIfAbsent();
        } else {
            // id is already associated with entity key
            isDone = true;
        }
    }

    /*
     * Process stage 2 (put into vertex table)
     */
    private void putVertexIfAbsent() {
        ResultSet rs = gqs.putVertexIfAbsentAsync( id, reference ).getUninterruptibly();
        putVertexIfAbsentResult( rs );
    }

    /*
     * Handle what happens after stage 2 (put into vertex table) is finished.
     */
    private void putVertexIfAbsentResult( ResultSet rs ) {
        if ( Util.wasLightweightTransactionApplied( rs ) ) {
            // put Vertex table is successful
            isDone = true;
        } else {
            // uuid is already used, need to repick uuid
            updateVertexLookupIfExists();
        }
    }

    /*
     * Process stage 3 (update id of vertex lookup table)
     */
    private void updateVertexLookupIfExists() {
        id = UUID.randomUUID();
        ResultSet rs = gqs.updateVertexLookupIfExistsAsync( id, reference ).getUninterruptibly();
        putVertexLookupIfAbsentResult( rs );
    }

}
