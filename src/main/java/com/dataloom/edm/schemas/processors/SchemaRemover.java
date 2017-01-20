package com.dataloom.edm.schemas.processors;

import com.kryptnostic.rhizome.hazelcast.objects.DelegatedStringSet;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRemover;

public class SchemaRemover extends AbstractRemover<String, DelegatedStringSet, String> {
    private static final long serialVersionUID = 28849285823694547L;

    public SchemaRemover( Iterable<String> objectsToRemove ) {
        super( objectsToRemove );
    }
}
