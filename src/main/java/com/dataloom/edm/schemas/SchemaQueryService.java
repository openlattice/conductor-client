package com.dataloom.edm.schemas;

import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

public interface SchemaQueryService {

    Set<UUID> getAllPropertyTypesInSchema( FullQualifiedName schemaName );

    Set<UUID> getAllEntityTypesInSchema( FullQualifiedName schemaName );

}