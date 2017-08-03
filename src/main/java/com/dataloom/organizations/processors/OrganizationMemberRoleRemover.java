package com.dataloom.organizations.processors;

import com.dataloom.authorization.Principal;
import com.dataloom.organizations.PrincipalSet;
import com.kryptnostic.rhizome.hazelcast.objects.SetProxy;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRemover;

import java.util.Map;
import java.util.UUID;

public class OrganizationMemberRoleRemover extends AbstractRemover<UUID, PrincipalSet, Principal> {

    private static final long serialVersionUID = 6100482436786837269L;

    public OrganizationMemberRoleRemover( Iterable<Principal> objects ) {
        super( objects );
    }

    @Override
    public Void process( Map.Entry<UUID, PrincipalSet> entry ) {

        PrincipalSet currentObjects = entry.getValue();
        if ( currentObjects != null ) {
            for ( Principal objectToRemove : objectsToRemove ) {
                currentObjects.remove( objectToRemove );
            }
        }

        if ( !( currentObjects instanceof SetProxy<?, ?> ) ) {
            entry.setValue( currentObjects );
        }

        return null;
    }
}