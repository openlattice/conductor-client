package com.dataloom.edm.types.processors;

import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import com.openlattice.edm.EntitySet;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

public class UpdateEntitySetContactsProcessor extends AbstractRhizomeEntryProcessor<UUID, EntitySet, Object> {
    private static final long serialVersionUID = 4846226537808942435L;
    private final Set<String> contacts;

    public UpdateEntitySetContactsProcessor( Set<String> contacts ) {
        this.contacts = contacts;
    }

    @Override
    public Object process( Entry<UUID, EntitySet> entry ) {
        EntitySet es = entry.getValue();
        if ( es != null ) {
            es.setContacts( contacts );
            entry.setValue( es );
        }
        return null;
    }

    public Set<String> getContacts() {
        return contacts;
    }

}
