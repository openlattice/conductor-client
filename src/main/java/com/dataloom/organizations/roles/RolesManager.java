package com.dataloom.organizations.roles;

import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.directory.pojo.Auth0UserBasic;
import com.openlattice.authorization.SecurablePrincipal;
import java.util.Collection;
import java.util.UUID;

public interface RolesManager {

    /**
     * @param owner The owner of a role. Usually the organization.
     * @param principal The principal which to create.
     */
    void createSecurablePrincipalIfNotExists( Principal owner, SecurablePrincipal principal );

    SecurablePrincipal getPrincipal( Principal principal );

    Collection<SecurablePrincipal> getPrincipals( PrincipalType principalType );

    Collection<SecurablePrincipal> getAllRolesInOrganization( UUID organizationId );

    void updateTitle( Principal principal, String title );

    void updateDescription( Principal principal, String description );

    void deletePrincipal( Principal principal );

    void deleteAllRolesInOrganization( UUID organizationId, Iterable<Principal> users );

    void addPrincipalToPrincipal( Principal source, Principal target );

    void removePrincipalFromPrincipal( Principal source, Principal target );

    // Methods about users
    Collection<Principal> getAllUsersWithPrincipal( Principal principal );

    Collection<Auth0UserBasic> getAllUserProfilesWithPrincipal( Principal principal );
}
