package com.kryptnostic.conductor.rpc;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

public class LookupEntitiesRequest {
    private final Set<FullQualifiedName>         entityTypes;
    private final Map<FullQualifiedName, Object> propertyTypeToValueMap;
    private final UUID                           userId;

    public LookupEntitiesRequest(
            UUID userId,
            Set<FullQualifiedName> entityTypes,
            Map<FullQualifiedName, Object> propertyTypeToValueMap ) {
        this.entityTypes = entityTypes;
        this.propertyTypeToValueMap = propertyTypeToValueMap;
        this.userId = userId;
    }

    public Set<FullQualifiedName> getEntityTypes() {
        return entityTypes;
    }

    public Map<FullQualifiedName, Object> getPropertyTypeToValueMap() {
        return propertyTypeToValueMap;
    }

    public UUID getUserId() {
        return userId;
    }

}
