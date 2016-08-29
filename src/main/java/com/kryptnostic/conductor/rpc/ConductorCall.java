package com.kryptnostic.conductor.rpc;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.Callable;

import com.google.common.base.Preconditions;

public class ConductorCall<T> implements Callable<T>, Serializable {
    private static final long       serialVersionUID = 7100103643538018543L;
    private final UUID              userId;
    private final Callable<T>       call;
    private final ConductorSparkApi api;

    public ConductorCall( UUID userId, Callable<T> call, ConductorSparkApi api ) {
        this.userId = Preconditions.checkNotNull( userId );
        this.call = Preconditions.checkNotNull( call );
        this.api = Preconditions.checkNotNull( api );
    }

    public UUID getUserId() {
        return userId;
    }

    @Override
    public T call() throws Exception {
        return call.call();
    }

    public static <T> ConductorCall<T> wrap( Callable<T> c ) {
        return new ConductorCall<T>( UUID.randomUUID(), c, null );
    }
}
