package com.dataloom.edm.schemas.processors;

import java.util.Set;

import com.google.common.collect.Sets;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractMerger;

public class SchemaMerger extends AbstractMerger<String, Set<String>, String> {
    private static final long serialVersionUID = 3220711286041457718L;

    public SchemaMerger( Iterable<String> objects ) {
        super( objects );
    }

    @Override
    protected Set<String> newEmptyCollection() {
        return Sets.newHashSet();
    }

}
