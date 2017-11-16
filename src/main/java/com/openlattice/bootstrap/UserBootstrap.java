package com.openlattice.bootstrap;

import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.directory.pojo.Auth0UserBasic;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.organizations.roles.SecurePrincipalsManager;
import com.google.common.base.Optional;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.DbCredentialService;
import com.openlattice.authorization.SecurablePrincipal;

import static com.google.common.base.Preconditions.checkState;

public class UserBootstrap {
    private static final String AUTHENTICATED_USER = "AuthenticatedUser";
    private static final String ADMIN              = "admin";

    private final IMap<String, Auth0UserBasic> users;

    private final SecurePrincipalsManager spm;
    private final DbCredentialService dbCredService;

    public UserBootstrap( HazelcastInstance hazelcast, SecurePrincipalsManager spm, DbCredentialService dbCredService ) {
        this.users = hazelcast.getMap( HazelcastMap.USERS.name() );
        this.spm = spm;
        this.dbCredService = dbCredService;

        AclKey userRoleAclKey = spm.lookup( AuthorizationBootstrap.GLOBAL_USER_ROLE.getPrincipal() );
        AclKey adminRoleAclKey = spm.lookup( AuthorizationBootstrap.GLOBAL_ADMIN_ROLE.getPrincipal() );

        users.values().parallelStream().forEach( user -> {
            String userId = user.getUserId();
            Principal principal = new Principal( PrincipalType.USER, userId );

            if (!spm.principalExists( principal ) ) {
                checkState( user.getUserId().equals( userId ), "Retrieved user id must match submitted user id" );
                dbCredService.createUser( userId );
                spm.createSecurablePrincipalIfNotExists( principal,
                        new SecurablePrincipal( Optional.absent(), principal, user.getNickname(), Optional.absent() ) );

            }

            AclKey userAclKey = spm.lookup( principal );

            if ( user.getRoles().contains( AUTHENTICATED_USER ) ) {
                spm.addPrincipalToPrincipal( userRoleAclKey, userAclKey );
            }

            if ( user.getRoles().contains( ADMIN ) ) {
                spm.addPrincipalToPrincipal( adminRoleAclKey, userAclKey );
            }

        } );

    }

}
