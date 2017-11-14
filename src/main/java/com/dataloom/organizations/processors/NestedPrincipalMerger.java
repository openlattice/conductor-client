package com.dataloom.organizations.processors;

import com.kryptnostic.rhizome.hazelcast.processors.AbstractMerger;
import com.openlattice.authorization.AclKeySet;
import java.util.List;
import java.util.UUID;

public class NestedPrincipalMerger extends AbstractMerger<List<UUID>, AclKeySet, List<UUID>> {

    private static final long serialVersionUID = 4127837036304775975L;

    public NestedPrincipalMerger( Iterable<List<UUID>> principalsToAdd ) {
        super( principalsToAdd );
    }

    @Override
    protected AclKeySet newEmptyCollection() {
        return new AclKeySet();
    }
}
