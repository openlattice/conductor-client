package com.dataloom.organizations.processors;

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRemover;
import com.openlattice.authorization.AclKeySet;
import java.util.List;
import java.util.UUID;

public class NestedPrincipalRemover extends AbstractRemover<List<UUID>, AclKeySet, List<UUID>> {

    private static final long serialVersionUID = 6100482436786837269L;

    public NestedPrincipalRemover( Iterable<List<UUID>> principalsToRemove ) {
        super( principalsToRemove );
    }

}