package com.dataloom.organizations.roles.processors;

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;
import com.openlattice.authorization.SecurablePrincipal;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;

public class PrincipalTitleUpdater extends AbstractRhizomeEntryProcessor<List<UUID>, SecurablePrincipal, Object> {
    private static final long serialVersionUID = -717197511031518227L;
    private final String newTitle;

    public PrincipalTitleUpdater( String newTitle ) {
        this.newTitle = newTitle;
    }

    @Override
    public Object process( Entry<List<UUID>, SecurablePrincipal> entry ) {
        SecurablePrincipal principal = entry.getValue();
        if ( principal != null ) {
            principal.setTitle( newTitle );
            //Need to let Hazelcast know to persist title update
            entry.setValue( principal );
        }
        return null;
    }

    public String getTitle() {
        return newTitle;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( newTitle == null ) ? 0 : newTitle.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) { return true; }
        if ( obj == null ) { return false; }
        if ( getClass() != obj.getClass() ) { return false; }
        PrincipalTitleUpdater other = (PrincipalTitleUpdater) obj;
        if ( newTitle == null ) {
            if ( other.newTitle != null ) { return false; }
        } else if ( !newTitle.equals( other.newTitle ) ) { return false; }
        return true;
    }

}