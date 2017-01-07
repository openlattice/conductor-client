package com.dataloom.authorization;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.authorization.requests.Permission;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
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
        AclKey esKey = new AclKey( SecurableObjectType.EntitySet, entitySetId );
        return selectedProperties.stream()
                .filter( ptId -> authz.checkIfHasPermissions( Arrays.asList( esKey,
                        new AclKey( SecurableObjectType.PropertyTypeInEntitySet, ptId ) ),
                        Principals.getCurrentPrincipals(),
                        requiredPermissions ) )
                .collect( Collectors.toSet() );
    }

    public Set<UUID> getAllPropertiesOnEntitySet( UUID entitySetId ) {
        EntitySet es = edm.getEntitySet( entitySetId );
        // TODO Fix after BACKEND-612
        // EntityType et = dms.getEntityType( es.getTypeId() );
        EntityType et = edm.getEntityType( es.getType() );
        
        return et.getProperties();
    }
    
    /**
     * Static helper methods for List &lt; AclKey &gt; creation.
     */
    
    public static List<AclKey> getSecurableObjectPath( SecurableObjectType objType, UUID objId ){
        return Arrays.asList( new AclKey( objType, objId ) );
    }
    
}
