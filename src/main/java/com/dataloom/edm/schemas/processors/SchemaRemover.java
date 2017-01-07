package com.dataloom.edm.schemas.processors;

import java.util.Set;

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRemover;

public class SchemaRemover extends AbstractRemover<String, Set<String>, String> {
    private static final long serialVersionUID = 28849285823694547L;

    public SchemaRemover( Iterable<String> objectsToRemove ) {
        super( objectsToRemove );
    }
}
