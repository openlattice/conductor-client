package com.kryptnostic.conductor.rpc;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

public class EntityDataLambdas implements Function<ConductorSparkApi, Boolean>, Serializable {
    private static final long serialVersionUID = -1071651645473672891L;
    
    private UUID entitySetId;
    private String entityId;
    private Map<UUID, String> propertyValues;

    public EntityDataLambdas( UUID entitySetId, String entityId, Map<UUID, String> propertyValues ) {
        this.entitySetId = entitySetId;
        this.entityId = entityId;
        this.propertyValues = propertyValues;
    }

    @Override
    public Boolean apply( ConductorSparkApi api ) {
        return api.createEntityData( entitySetId, entityId, propertyValues );
    }

}
