package com.dataloom.organizations.roles.processors;

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;
import com.openlattice.authorization.SecurablePrincipal;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;

public class PrincipalDescriptionUpdater extends AbstractRhizomeEntryProcessor<List<UUID>, SecurablePrincipal, Object> {
    private static final long serialVersionUID = -1888534083122168784L;
    private final String newDescription;

    public PrincipalDescriptionUpdater( String newDescription ) {
        this.newDescription = newDescription;
    }

    @Override
    public Object process( Entry<List<UUID>, SecurablePrincipal> entry ) {
        SecurablePrincipal principal = entry.getValue();
        if ( principal != null ) {
            principal.setDescription( newDescription );
            entry.setValue( principal );
        }
        return null;
    }

    public String getDescription() {
        return newDescription;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( newDescription == null ) ? 0 : newDescription.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) { return true; }
        if ( obj == null ) { return false; }
        if ( getClass() != obj.getClass() ) { return false; }
        PrincipalDescriptionUpdater other = (PrincipalDescriptionUpdater) obj;
        if ( newDescription == null ) {
            if ( other.newDescription != null ) { return false; }
        } else if ( !newDescription.equals( other.newDescription ) ) { return false; }
        return true;
    }

}