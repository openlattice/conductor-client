package com.dataloom.organizations.roles;

import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.directory.pojo.Auth0UserBasic;
import com.dataloom.organization.roles.Role;
import com.google.common.collect.SetMultimap;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.query.Predicate;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.SecurablePrincipal;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public interface SecurePrincipalsManager {

    /**
     * @param owner The owner of a role. Usually the organization.
     * @param principal The principal which to create.
     */
    void createSecurablePrincipalIfNotExists( Principal owner, SecurablePrincipal principal );

    //    SecurablePrincipal getSecurablePrincipal( Principal principal );

    /**
     * Retrieves a securable principal by acl key lookup.
     *
     * @param aclKey The acl key for the securable principal.
     * @return The securable principal identified by acl key.
     */
    SecurablePrincipal getSecurablePrincipal( AclKey aclKey );

    SecurablePrincipal getPrincipal( String principalId );

    Collection<SecurablePrincipal> getSecurablePrincipals( PrincipalType principalType );

    Collection<SecurablePrincipal> getAllRolesInOrganization( UUID organizationId );

    SetMultimap<SecurablePrincipal,SecurablePrincipal> getRolesForUsersInOrganization( UUID organizationId );

    Collection<SecurablePrincipal> getSecurablePrincipals( Predicate p );

    void createSecurablePrincipal(
            Principal owner, SecurablePrincipal principal );

    void updateTitle( AclKey aclKey, String title );

    void updateDescription( AclKey aclKey, String description );

    void deletePrincipal( AclKey aclKey );

    void deleteAllRolesInOrganization( UUID organizationId );

    void addPrincipalToPrincipal( AclKey source, AclKey target );

    void removePrincipalFromPrincipal( AclKey source, AclKey target );

    Map<AclKey, Object> executeOnPrincipal( EntryProcessor<AclKey, SecurablePrincipal> ep, Predicate p );

    //More logical to use Principal

    void removePrincipalFromPrincipals( AclKey source, Predicate targetFilter );

    Collection<SecurablePrincipal> getAllPrincipalsWithPrincipal( AclKey aclKey );

    // Methods about users
    Collection<Principal> getAllUsersWithPrincipal( AclKey principal );

    Collection<Auth0UserBasic> getAllUserProfilesWithPrincipal( AclKey principal );

    boolean principalExists( Principal p );

    Auth0UserBasic getUser( String userId );

    Role getRole( UUID organizationId, UUID roleId );

    AclKey lookup( Principal p );

    Collection<Principal> getPrincipals( Predicate<AclKey, SecurablePrincipal> p );

    Collection<SecurablePrincipal> getSecurablePrincipals( Set<Principal> members );

    Collection<SecurablePrincipal> getAllPrincipals( SecurablePrincipal sp );
}
