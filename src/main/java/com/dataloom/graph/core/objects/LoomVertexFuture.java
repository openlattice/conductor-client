package com.dataloom.graph.core.objects;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.core.GraphQueryService;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.datastore.util.Util;

/**
 * Initialize by entity key; when get is called, should return a definite LoomVertex (i.e. the UUID must be obtained)
 * 
 * @author Ho Chung Siu
 *
 */
public class LoomVertexFuture implements ListenableFuture<LoomVertex> {
    private static final Logger      logger          = LoggerFactory.getLogger( LoomVertexFuture.class );
    private static GraphQueryService gqs;
    // TODO use designated threadpool to handle those async calls
    private static final Executor    executor;

    private EntityKey                reference;
    private UUID                     id;

    private ResultSetFuture          vertexLookupRsf;
    private ResultSetFuture          vertexRsf;

    private boolean                  putVertexLookup = false;
    private boolean                  isDone          = false;

    private Runnable                 callbackRunnable;
    private Executor                 callbackExecutor;

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
                    executeCallback();
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

    @Override
    public boolean cancel( boolean mayInterruptIfRunning ) {
        return vertexLookupRsf.cancel( mayInterruptIfRunning ) && vertexRsf.cancel( mayInterruptIfRunning );
    }

    @Override
    public boolean isCancelled() {
        return vertexLookupRsf.isCancelled() || vertexRsf.isCancelled();
    }

    @Override
    public boolean isDone() {
        return isDone;
    }

    @Override
    public LoomVertex get() throws InterruptedException, ExecutionException {
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
    private void putVertexLookupIfAbsentResult( ResultSet rs ) {
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

    @Override
    public LoomVertex get( long timeout, TimeUnit unit )
            throws InterruptedException, ExecutionException, TimeoutException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void addListener( Runnable listener, Executor executor ) {
        this.callbackRunnable = listener;
        this.callbackExecutor = executor;
        if ( isDone ) {
            executeCallback();
        }
    }

    private void executeCallback() {
        if ( callbackRunnable != null && callbackExecutor != null ) {
            callbackExecutor.execute( callbackRunnable );
        }
    }

    public LoomVertex getUninterruptibly() {
        try {
            return get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "Getting LoomVertex with id {} failed because of exception {}.",
                    id,
                    e.getClass().toString() );
            return null;
        }
    }

}
