package com.kryptnostic.conductor.rpc;

import java.io.Serializable;
import java.util.List;

public class Lambdas {
    public static Runnable foo() {
        return (Runnable & Serializable) () -> System.out.println( "UNSTOPPABLE" );
    }

    public static ConductorCall getEmployees() {
        return new ConductorCall() {
            private static final long serialVersionUID = 3766075442981764029L;

            @Override
            public List<Employee> call() throws Exception {
                return api.processEmployees();
            }
        };
    }
}
