package com.kryptnostic.conductor.rpc;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Function;

import com.google.common.base.Preconditions;

public class ConductorCall<T> implements Callable<T>, Serializable {
    private static final long                    serialVersionUID = 7100103643538018543L;
    private final UUID                           userId;
    private final Function<ConductorSparkApi, T> f;
    private final ConductorSparkApi              api;

    public ConductorCall( UUID userId, Function<ConductorSparkApi, T> call, ConductorSparkApi api ) {
        this.userId = Preconditions.checkNotNull( userId );
        this.f = Preconditions.checkNotNull( call );
        this.api = api;
    }

    public UUID getUserId() {
        return userId;
    }

    @Override
    public T call() throws Exception {
        return f.apply( api );
    }

    public static <T> ConductorCall<T> wrap( Function<ConductorSparkApi, T> f ) {
        return new ConductorCall<T>( UUID.randomUUID(), f, null );
    }

    public Function<ConductorSparkApi, T> getFunction() {
        return f;
    }
}
