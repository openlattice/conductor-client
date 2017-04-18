package com.kryptnostic.datastore.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.core.ICompletableFuture;

public class FuturesAdapter {
    private FuturesAdapter(){}
    
    /*
     * Based on Guava Futures.addCallback Impl
     */
    public <T> ListenableFuture<T> toListenableFuture( ICompletableFuture<T> hzFuture ){
    }
    
    public static class ICompletableFutureWrapper<T> implements ListenableFuture<T> {
        private final ICompletableFuture<T> future;

        ICompletableFutureWrapper( ICompletableFuture<T> future ){
            this.future = future;            
        }
        
        @Override
        public boolean cancel( boolean mayInterruptIfRunning ) {
            return future.cancel( mayInterruptIfRunning );
        }

        @Override
        public boolean isCancelled() {
            return future.isCancelled();
        }

        @Override
        public boolean isDone() {
            return future.isDone();
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            return future.get();
        }

        @Override
        public T get( long timeout, TimeUnit unit ) throws InterruptedException, ExecutionException, TimeoutException {
            return future.get( timeout, unit );
        }

        @Override
        public void addListener( Runnable listener, Executor executor ) {
            
        }
    }
}
