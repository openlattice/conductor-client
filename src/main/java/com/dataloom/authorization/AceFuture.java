package com.dataloom.authorization;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;

public class AceFuture implements ListenableFuture<Ace> {
    private static final Logger                       logger = LoggerFactory.getLogger( AceFuture.class );
    private final Principal                           principal;
    private final ICompletableFuture<Set<Permission>> futurePermissions;

    public AceFuture( Principal principal, ICompletableFuture<Set<Permission>> futurePermissions ) {
        this.principal = principal;
        this.futurePermissions = futurePermissions;
    }

    @Override
    public boolean cancel( boolean mayInterruptIfRunning ) {
        return futurePermissions.cancel( mayInterruptIfRunning );
    }

    @Override
    public boolean isCancelled() {
        return futurePermissions.isCancelled();
    }

    @Override
    public boolean isDone() {
        return futurePermissions.isDone();
    }

    @Override
    public Ace get() throws InterruptedException, ExecutionException {
        return new Ace( principal, futurePermissions.get() );
    }

    public Ace getUninterruptibly() {
        try {
            return get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Failed to get Ace", e );
            return null;
        }
    }

    @Override
    public Ace get( long timeout, TimeUnit unit ) throws InterruptedException, ExecutionException, TimeoutException {
        return new Ace( principal, futurePermissions.get( timeout, unit ) );
    }

    @Override
    public void addListener( Runnable listener, Executor executor ) {
        futurePermissions.andThen( new ExecutionCallback<Set<Permission>>() {

            @Override
            public void onResponse( Set<Permission> response ) {
                listener.run();
            }

            @Override
            public void onFailure( Throwable t ) {
                logger.error( "Unable to retrieve Ace.", t );
            }
        }, executor );
    }
}
