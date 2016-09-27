package com.kryptnostic.conductor.rpc;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Function;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

public class Lambdas implements Serializable {
    private static final long serialVersionUID = -8384320983731367620L;

    public static Runnable foo() {
        return (Runnable & Serializable) () -> System.out.println( "UNSTOPPABLE" );
    }
    
    public static Function<ConductorSparkApi, QueryResult> getAllEntitiesOfType( FullQualifiedName fqn ) {
    	return (Function<ConductorSparkApi, QueryResult> & Serializable) (api) -> api.loadAllEntitiesOfType( fqn );
    }
    
    public static Function<ConductorSparkApi, QueryResult> getFilteredEntities( LookupEntitiesRequest lookupEntitiesRequest ) {
    	return (Function<ConductorSparkApi, QueryResult> & Serializable) (api) -> api.filterEntities( lookupEntitiesRequest );
    }
    
//    public static Function<ConductorSparkApi, QueryResult> getAllEntitiesOfType(final FullQualifiedName fqn ) {
//        return (Function<ConductorSparkApi, QueryResult> & Serializable) (api) -> api.loadAllEntitiesOfType( fqn );
//    }
//    public static Callable<List<Employee>> getEmployees() {
//        return new ConductorCall() {
//            private static final long serialVersionUID = 3766075442981764029L;
//
//            @Override
//            public List<Employee> call() throws Exception {
//                return null;
//            }
//        };
}