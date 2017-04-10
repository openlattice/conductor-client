package com.dataloom.graph.core.objects;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
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
import com.google.common.util.concurrent.MoreExecutors;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.datastore.util.Util;

/**
 * Initialize by entity key; when get is called, should return a definite LoomVertex (i.e. the UUID must be obtained)
 * @author Ho Chung Siu
 *
 */
public class LoomVertexFuture implements ListenableFuture<LoomVertex> {
    private static final Logger     logger = LoggerFactory.getLogger( LoomVertexFuture.class );
    private static GraphQueryService gqs;
    private static final Executor executor = MoreExecutors.directExecutor();

    private EntityKey reference;
    private UUID id;

    private ResultSetFuture vertexLookupRsf;
    private ResultSetFuture vertexRsf;

    private boolean isDone = false;
    
    public static void setGraphQueryService( GraphQueryService gqs ){
        LoomVertexFuture.gqs = gqs;
    }

    public LoomVertexFuture( EntityKey reference ) {
        this.reference = reference;
        this.id = UUID.randomUUID();
        putVertexLookupIfAbsent( id, reference );
    }

    private void putVertexLookupIfAbsent( UUID id, EntityKey reference ){
        vertexLookupRsf = gqs.putVertexLookUpIfAbsent( id, reference );
        addCallbackToVertexLookup();
    }

    private void putVertexIfAbsent( UUID id, EntityKey reference ){
        vertexRsf = gqs.putVertexIfAbsent( id, reference );
        addCallbackToVertex();
    }

    private void addCallbackToVertexLookup(){
        Futures.addCallback(vertexLookupRsf, new FutureCallback<ResultSet>(){
            @Override public void onSuccess( ResultSet rs ){
                if( Util.wasLightweightTransactionApplied( rs ) ){
                    putVertexIfAbsent( id, reference );
                } else {
                    id = RowAdapters.vertexId( rs.one() );
                    isDone = true;
                }
            }
            
            @Override public void onFailure( Throwable t ){
                id = UUID.randomUUID();
                putVertexLookupIfAbsent( id, reference );
            }
        } );
    }
    
    private void addCallbackToVertex(){
        Futures.addCallback(vertexRsf, new FutureCallback<ResultSet>(){
            @Override public void onSuccess( ResultSet rs ){
                if( Util.wasLightweightTransactionApplied( rs ) ){
                    isDone = true;
                } else {
                    //uuid is already being used
                    id = UUID.randomUUID();
                    putVertexLookupIfAbsent( id, reference );                    
                }
            }
            
            @Override public void onFailure( Throwable t ){
                putVertexIfAbsent( id, reference );
            }
        } );
    }

    @Override
    public boolean cancel( boolean mayInterruptIfRunning ) {
        return vertexLookupRsf.cancel( mayInterruptIfRunning ) && vertexRsf.cancel( mayInterruptIfRunning );
    }

    @Override
    public boolean isCancelled() {
        return vertexLookupRsf.isCancelled() && vertexRsf.isCancelled();
    }

    @Override
    public boolean isDone() {
        return isDone;
    }

    @Override
    public LoomVertex get() throws InterruptedException, ExecutionException {
        if( isDone ){
            return new LoomVertex( id, reference );
        }
        
        if( vertexLookupRsf.isDone() ){
            ResultSet rs = vertexRsf.getUninterruptibly();
            if( Util.wasLightweightTransactionApplied( rs ) ){
                return new LoomVertex( id, reference );
            } else {
                //UUID is already used; need to repick UUID
            }
        } else {
            // get the lookup rsf first
            ResultSet rs1 = vertexLookupRsf.getUninterruptibly();
        }
        
        while( true ){
            id = UUID.randomUUID();
            ResultSet rs1 = gqs.putVertexLookUpIfAbsent( id, reference ).getUninterruptibly();
            if( Util.wasLightweightTransactionApplied( rs1 ) ){
                ResultSet rs2 = gqs.putVertexIfAbsent( id, reference ).getUninterruptibly();
                if( Util.wasLightweightTransactionApplied( rs2 ) ){
                    return new LoomVertex( id, reference );
                } else {
                    //UUID is already used, need to repick UUID; restart.
                }
            } else {
                actualId = RowAdapters.vertexId( rs1.one() );
                return new LoomVertex( actualId, reference );                        
            }
        }

    }

    @Override
    public LoomVertex get( long timeout, TimeUnit unit )
            throws InterruptedException, ExecutionException, TimeoutException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void addListener( Runnable listener, Executor executor ) {
        // TODO Auto-generated method stub
        
    }
}
