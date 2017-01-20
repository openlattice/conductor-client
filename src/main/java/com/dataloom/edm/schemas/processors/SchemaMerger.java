package com.dataloom.edm.schemas.processors;

import com.google.common.collect.Sets;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedStringSet;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractMerger;

public class SchemaMerger extends AbstractMerger<String, DelegatedStringSet, String> {
    private static final long serialVersionUID = 3220711286041457718L;

    public SchemaMerger( Iterable<String> objects ) {
        super( objects );
    }

    @Override
    protected DelegatedStringSet newEmptyCollection() {
        return DelegatedStringSet.wrap( Sets.newHashSet() );
    }

}
