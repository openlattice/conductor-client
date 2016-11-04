package com.kryptnostic.conductor.rpc;

import java.io.Serializable;
import java.util.List;
import java.util.function.Function;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.kryptnostic.conductor.rpc.odata.PropertyType;

public class GetAllEntitiesOfTypeLambda implements Function<ConductorSparkApi, QueryResult>, Serializable {

    private static final long serialVersionUID = 1L;

    private FullQualifiedName fqn;
    private List<PropertyType> properties;

    public GetAllEntitiesOfTypeLambda( FullQualifiedName fqn, List<PropertyType> authorizedProperties ) {
        this.fqn = fqn;
        this.properties = authorizedProperties;
    }

    @Override
    public QueryResult apply( ConductorSparkApi api ) {
        return api.getAllEntitiesOfType( fqn, properties );
    }
}