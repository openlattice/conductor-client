package com.openlattice.bootstrap;

import static com.google.common.base.Preconditions.checkState;

import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.authorization.SystemRole;
import com.dataloom.directory.pojo.Auth0UserBasic;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.organizations.roles.SecurePrincipalsManager;
import com.google.common.base.Optional;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.DbCredentialService;
import com.openlattice.authorization.SecurablePrincipal;
import com.openlattice.authorization.mapstores.UserMapstore;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserBootstrap {
    private static final Logger logger = LoggerFactory.getLogger( UserBootstrap.class );

    public UserBootstrap(
            HazelcastInstance hazelcastInstance,
            SecurePrincipalsManager spm,
            DbCredentialService dbCredService ) throws InterruptedException {
        logger.info( "Starting activation of existing auth0 users." );
        IMap<String, Auth0UserBasic> users = hazelcastInstance.getMap( HazelcastMap.USERS.name() );
        IAtomicLong nextTime = hazelcastInstance.getAtomicLong( UserMapstore.class.getCanonicalName() );
        AclKey userRoleAclKey = spm.lookup( AuthorizationBootstrap.GLOBAL_USER_ROLE.getPrincipal() );
        AclKey adminRoleAclKey = spm.lookup( AuthorizationBootstrap.GLOBAL_ADMIN_ROLE.getPrincipal() );

        while ( nextTime.get() == 0 ) {
            logger.warn( "Waiting for User mapstore to be initialized..." );
            Thread.sleep( 1000 );
        }

        logger.info( "Starting processing of {} users.", users.size() );

        for ( Entry<String, Auth0UserBasic> userEntry : users.entrySet() ) {
            String userId = userEntry.getKey();
            Auth0UserBasic user = userEntry.getValue();
            Principal principal = new Principal( PrincipalType.USER, userId );

            if ( user != null ) {
                if ( !spm.principalExists( principal ) ) {
                    checkState( user.getUserId().equals( userId ),
                            "Retrieved user id must match submitted user id" );
                    logger.info( "Activating user {}", userId );
                    dbCredService.createUser( userId );
                    String title = ( user.getNickname() != null && user.getNickname().length() > 0 ) ?
                            user.getNickname() :
                            user.getEmail();
                    spm.createSecurablePrincipalIfNotExists( principal,
                            new SecurablePrincipal( Optional.absent(), principal, title, Optional.absent() ) );

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
}
