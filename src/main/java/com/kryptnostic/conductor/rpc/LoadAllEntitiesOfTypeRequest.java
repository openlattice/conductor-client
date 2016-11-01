package com.kryptnostic.conductor.rpc;

import java.util.Map;

import com.datastax.driver.core.DataType;

public class LoadAllEntitiesOfTypeRequest {
    public String                entityType;
    public Map<String, Class<?>> propertyTypeTables;
}
