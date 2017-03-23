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

}