package com.dataloom.organizations.processors;

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRemover;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AclKeySet;
import java.util.List;
import java.util.UUID;

public class NestedPrincipalRemover extends AbstractRemover<AclKey, AclKeySet, AclKey> {

    private static final long serialVersionUID = 6100482436786837269L;

    public NestedPrincipalRemover( Iterable<AclKey> principalsToRemove ) {
        super( principalsToRemove );
    }

}