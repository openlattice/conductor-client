package com.dataloom.organizations.processors;

import com.dataloom.authorization.Principal;
import com.dataloom.organizations.PrincipalSet;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedStringSet;
import com.kryptnostic.rhizome.hazelcast.objects.SetProxy;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRemover;

import java.util.Map;
import java.util.UUID;

public class NestedPrincipalRemover extends AbstractRemover<Principal, PrincipalSet, Principal> {

    private static final long serialVersionUID = 6100482436786837269L;

    public NestedPrincipalRemover( Iterable<Principal> principalsToRemove ) {
        super( principalsToRemove );
    }


}