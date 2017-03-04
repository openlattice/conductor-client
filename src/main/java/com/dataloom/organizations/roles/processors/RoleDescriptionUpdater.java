package com.dataloom.organizations.roles.processors;

import java.util.Map.Entry;
import java.util.UUID;

import com.dataloom.organization.roles.OrganizationRole;
import com.google.common.base.Optional;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

public class RoleDescriptionUpdater extends AbstractRhizomeEntryProcessor<UUID, OrganizationRole, Object> {
    private static final long serialVersionUID = -1888534083122168784L;
    private final String      newDescription;

    public RoleDescriptionUpdater( String newDescription ) {
        this.newDescription = newDescription;
    }

    @Override
    public Object process( Entry<UUID, OrganizationRole> entry ) {
        OrganizationRole role = entry.getValue();
        if ( role != null ) {
            OrganizationRole newRole = new OrganizationRole(
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

}