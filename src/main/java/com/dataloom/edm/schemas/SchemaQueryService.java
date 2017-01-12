package com.dataloom.edm.schemas;

import java.util.Set;
import java.util.UUID;

import javax.annotation.Nonnull;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

public interface SchemaQueryService {

    @Nonnull
    Set<UUID> getAllPropertyTypesInSchema( FullQualifiedName schemaName );

    @Nonnull
    Set<UUID> getAllEntityTypesInSchema( FullQualifiedName schemaName );

    @Nonnull
    Iterable<String> getNamespaces();

}