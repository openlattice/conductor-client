package com.kryptnostic.conductor.rpc;

import java.util.Map;

public class LoadAllEntitiesOfTypeRequest {
    public String                entityType;
    public Map<String, Class<?>> propertyTypeTables;
}
