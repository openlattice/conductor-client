package com.kryptnostic.conductor.rpc;

import java.util.Map;
import java.util.UUID;

public class LoadEntitiesRequest {
    private final Map<String, Object> propertyTableToValueMap;
    private final UUID                userId;

    public LoadEntitiesRequest( UUID userId, Map<String, Object> propertyTableToValueMap ) {
        this.propertyTableToValueMap = propertyTableToValueMap;
        this.userId = userId;
    }

    public Map<String, Object> getPropertyTableToValueMap() {
        return propertyTableToValueMap;
    }

    public UUID getUserId() {
        return userId;
    }
}
