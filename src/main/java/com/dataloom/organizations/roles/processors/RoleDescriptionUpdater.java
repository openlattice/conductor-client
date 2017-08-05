package com.dataloom.organizations.roles.processors;

import java.util.Map.Entry;

import com.dataloom.organization.roles.Role;
import com.dataloom.organization.roles.RoleKey;
import com.google.common.base.Optional;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

public class RoleDescriptionUpdater extends AbstractRhizomeEntryProcessor<RoleKey, Role, Object> {
    private static final long serialVersionUID = -1888534083122168784L;
    private final String      newDescription;

    public RoleDescriptionUpdater( String newDescription ) {
        this.newDescription = newDescription;
    }

    @Override
    public Object process( Entry<RoleKey, Role> entry ) {
        Role role = entry.getValue();
        if ( role != null ) {
            Role newRole = new Role(
                    Optional.of( role.getId() ),
                    role.getOrganizationId(),
                    role.getTitle(),
                    Optional.of( newDescription ) );
            entry.setValue( newRole );
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
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        RoleDescriptionUpdater other = (RoleDescriptionUpdater) obj;
        if ( newDescription == null ) {
            if ( other.newDescription != null ) return false;
        } else if ( !newDescription.equals( other.newDescription ) ) return false;
        return true;
    }

}