package com.dataloom.authorization;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.hazelcast.util.Preconditions;
import com.kryptnostic.datastore.services.EdmManager;

public class EdmAuthorizationHelper {

    private final EdmManager                    edm;
    private final HazelcastAuthorizationService authz;

    public EdmAuthorizationHelper( EdmManager edm, HazelcastAuthorizationService authz ) {
        this.edm = Preconditions.checkNotNull( edm );
        this.authz = Preconditions.checkNotNull( authz );
    }

    public Set<UUID> getAuthorizedPropertiesOnEntitySet(
            UUID entitySetId,
            EnumSet<Permission> requiredPermissions ) {
        return getAuthorizedPropertiesOnEntitySet(
                entitySetId,
                getAllPropertiesOnEntitySet( entitySetId ),
                requiredPermissions );
    }
    
    public Set<UUID> getAuthorizedPropertiesOnEntitySet(
            UUID entitySetId,
            Set<UUID> selectedProperties,
            EnumSet<Permission> requiredPermissions ) {
        AclKeyPathFragment esKey = new AclKeyPathFragment( SecurableObjectType.EntitySet, entitySetId );
        return selectedProperties.stream()
                .filter( ptId -> authz.checkIfHasPermissions( Arrays.asList( esKey,
                        new AclKeyPathFragment( SecurableObjectType.PropertyTypeInEntitySet, ptId ) ),
                        Principals.getCurrentPrincipals(),
                        requiredPermissions ) )
                .collect( Collectors.toSet() );
    }

    public Set<UUID> getAllPropertiesOnEntitySet( UUID entitySetId ) {
        EntitySet es = edm.getEntitySet( entitySetId );
        EntityType et = edm.getEntityType( es.getEntityTypeId() );
        return et.getProperties();
    }
    
    /**
     * Static helper methods for List &lt; AclKey &gt; creation.
     */
    
    public static List<AclKeyPathFragment> getSecurableObjectPath( SecurableObjectType objType, UUID objId ){
        return Arrays.asList( new AclKeyPathFragment( objType, objId ) );
    }
    
}
