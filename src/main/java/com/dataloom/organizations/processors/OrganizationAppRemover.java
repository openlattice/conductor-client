package com.dataloom.organizations.processors;

import com.kryptnostic.rhizome.hazelcast.objects.SetProxy;
import com.kryptnostic.rhizome.hazelcast.objects.UUIDSet;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRemover;

import java.util.Map;
import java.util.UUID;

public class OrganizationAppRemover extends AbstractRemover<UUID, UUIDSet, UUID> {
    private static final long serialVersionUID = 1095980528424382658L;

    public OrganizationAppRemover( Iterable<UUID> objectsToRemove ) {
        super( objectsToRemove );
    }

    @Override
    public Void process( Map.Entry<UUID, UUIDSet> entry ) {

        UUIDSet currentObjects = entry.getValue();
        if ( currentObjects != null ) {
            for ( UUID objectToRemove : objectsToRemove ) {
                currentObjects.remove( objectToRemove );
            }
        }

        if ( !( currentObjects instanceof SetProxy<?, ?> ) ) {
            entry.setValue( currentObjects );
        }

        return null;
    }
}
