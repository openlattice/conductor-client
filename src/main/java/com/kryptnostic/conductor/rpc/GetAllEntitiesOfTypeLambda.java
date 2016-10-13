package com.kryptnostic.conductor.rpc;

import java.io.Serializable;
import java.util.function.Function;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

public class GetAllEntitiesOfTypeLambda implements Function<ConductorSparkApi, QueryResult>, Serializable {

    private static final long serialVersionUID = 1L;

    private FullQualifiedName fqn;

    public GetAllEntitiesOfTypeLambda( FullQualifiedName fqn ) {
        this.fqn = fqn;
    }

    @Override
    public QueryResult apply( ConductorSparkApi api ) {
        return api.getAllEntitiesOfType( fqn );
    }
}