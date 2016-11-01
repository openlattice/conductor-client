package com.kryptnostic.conductor.rpc;

import java.io.Serializable;
import java.util.function.Function;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

public class Lambdas implements Serializable {
    private static final long serialVersionUID = -8384320983731367620L;

    public static Runnable foo() {
        return (Runnable & Serializable) () -> System.out.println( "UNSTOPPABLE" );
    }

    public static Function<ConductorSparkApi, QueryResult> getAllEntitiesOfType( FullQualifiedName fqn ) {
        return (Function<ConductorSparkApi, QueryResult> & Serializable) ( api ) -> api.getAllEntitiesOfType( fqn );
    }

    public static Function<ConductorSparkApi, QueryResult> getFilteredEntities(
            LookupEntitiesRequest lookupEntitiesRequest ) {
        return (Function<ConductorSparkApi, QueryResult> & Serializable) ( api ) -> api
                .getFilterEntities( lookupEntitiesRequest );
    }

    public static Function<ConductorSparkApi, QueryResult> getAllEntitiesOfEntitySet(
            FullQualifiedName entityFqn,
            String entitySetName ) {
        return (Function<ConductorSparkApi, QueryResult> & Serializable) ( api ) -> api
                .getAllEntitiesOfEntitySet( entityFqn, entitySetName );
    }
}