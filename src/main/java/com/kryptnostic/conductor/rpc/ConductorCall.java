package com.kryptnostic.conductor.rpc;

import java.io.Serializable;
import java.util.concurrent.Callable;

public abstract class ConductorCall<T> implements Callable<T>, Serializable {
    private static final long   serialVersionUID = 7100103643538018543L;
    protected ConductorSparkApi api;

    @Override
    public abstract T call();

    public void setApi( ConductorSparkApi api ) {
        this.api = api;
    }
}
