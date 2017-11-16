package com.openlattice.bootstrap;

import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.authorization.SystemRole;
import com.dataloom.directory.pojo.Auth0UserBasic;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.organizations.roles.SecurePrincipalsManager;
import com.google.common.base.Optional;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.DbCredentialService;
import com.openlattice.authorization.SecurablePrincipal;
import com.openlattice.authorization.mapstores.UserMapstore;
import digital.loom.rhizome.configuration.auth0.Auth0Configuration;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkState;

public class UserBootstrap {
    private final SecurePrincipalsManager spm;
    private final DbCredentialService     dbCredService;

    public UserBootstrap(
            Auth0Configuration auth0Configuration,
            SecurePrincipalsManager spm,
            DbCredentialService dbCredService ) {
        this.spm = spm;
        this.dbCredService = dbCredService;

        UserMapstore users = new UserMapstore();
        users.setToken( auth0Configuration.getToken() );

        AclKey userRoleAclKey = spm.lookup( AuthorizationBootstrap.GLOBAL_USER_ROLE.getPrincipal() );
        AclKey adminRoleAclKey = spm.lookup( AuthorizationBootstrap.GLOBAL_ADMIN_ROLE.getPrincipal() );

        for ( String userId : users.loadAllKeys() ) {
            Auth0UserBasic user = users.load( userId );
            Principal principal = new Principal( PrincipalType.USER, userId );

            if ( user != null ) {

                if ( !spm.principalExists( principal ) ) {
                    checkState( user.getUserId().equals( userId ), "Retrieved user id must match submitted user id" );
                    dbCredService.createUser( userId );
                    String title = ( user.getNickname() != null && user.getNickname().length() > 0 ) ?
                            user.getNickname() :
                            user.getEmail();
                    spm.createSecurablePrincipalIfNotExists( principal,
                            new SecurablePrincipal( Optional.absent(), principal, title, Optional.absent() ) );
                }

                AclKey userAclKey = spm.lookup( principal );

                if ( user.getRoles().contains( SystemRole.AUTHENTICATED_USER.getName() ) ) {
                    spm.addPrincipalToPrincipal( userRoleAclKey, userAclKey );
                }

                if ( user.getRoles().contains( SystemRole.ADMIN.getName() ) ) {
                    spm.addPrincipalToPrincipal( adminRoleAclKey, userAclKey );
                }
            }
        }
    }
}
