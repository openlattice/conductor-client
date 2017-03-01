package com.kryptnostic.conductor.rpc;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Function;

import com.google.common.base.Preconditions;

public class ConductorElasticsearchCall<T> implements Callable<T> {
    private final UUID                                   userId;
    private final Function<ConductorElasticsearchApi, T> f;
    private final ConductorElasticsearchApi              api;

    public ConductorElasticsearchCall(
            UUID userId,
            Function<ConductorElasticsearchApi, T> call,
            ConductorElasticsearchApi api ) {
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

    public static <T> ConductorElasticsearchCall<T> wrap( Function<ConductorElasticsearchApi, T> f ) {
        return new ConductorElasticsearchCall<T>( UUID.randomUUID(), f, null );
    }

    public Function<ConductorElasticsearchApi, T> getFunction() {
        return f;
    }
}
