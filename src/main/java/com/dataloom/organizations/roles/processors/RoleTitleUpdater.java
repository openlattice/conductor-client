package com.dataloom.organizations.roles.processors;

import java.util.Map.Entry;

import com.dataloom.organization.roles.OrganizationRole;
import com.dataloom.organization.roles.RoleKey;
import com.google.common.base.Optional;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

public class RoleTitleUpdater extends AbstractRhizomeEntryProcessor<RoleKey, OrganizationRole, Object> {
    private static final long serialVersionUID = -717197511031518227L;
    private final String      newTitle;

    public RoleTitleUpdater( String newTitle ) {
        this.newTitle = newTitle;
    }

    @Override
    public Object process( Entry<RoleKey, OrganizationRole> entry ) {
        OrganizationRole role = entry.getValue();
        if ( role != null ) {
            OrganizationRole newRole = new OrganizationRole(
                    Optional.of( role.getId() ),
                    role.getOrganizationId(),
                    newTitle,
                    Optional.of( role.getDescription() ) );
            entry.setValue( newRole );
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
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        RoleTitleUpdater other = (RoleTitleUpdater) obj;
        if ( newTitle == null ) {
            if ( other.newTitle != null ) return false;
        } else if ( !newTitle.equals( other.newTitle ) ) return false;
        return true;
    }

}