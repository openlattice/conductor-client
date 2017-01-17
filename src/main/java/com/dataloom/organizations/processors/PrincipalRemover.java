package com.dataloom.organizations.processors;

import java.util.UUID;

import com.dataloom.authorization.Principal;
import com.dataloom.organizations.PrincipalSet;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRemover;

public class PrincipalRemover extends AbstractRemover<UUID, PrincipalSet, Principal> {
    private static final long serialVersionUID = -5294948465220309317L;

    public PrincipalRemover( Iterable<Principal> objects ) {
        super( objects );
    }
}
