package com.dataloom.edm.schemas;

import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

public interface SchemaQueryService {

    Set<FullQualifiedName> getAllPropertyTypesInSchema( FullQualifiedName schemaName );

    Set<FullQualifiedName> getAllEntityTypesInSchema( FullQualifiedName schemaName );

}