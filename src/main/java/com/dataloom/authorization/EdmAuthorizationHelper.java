package com.dataloom.authorization;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;
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

    public Set<FullQualifiedName> getAuthorizedPropertiesOnEntitySet(
            EntitySet entitySet,
            Set<Permission> requiredPermissions ) {
        AclKey entityTypeAclKey = edm.getTypeAclKey( entitySet.getType() );
        EntityType entityType = edm.getEntityType( entityTypeAclKey.getId() );
        Collection<PropertyType> propertyType = edm.getPropertyTypes( entityType.getProperties() );
        return propertyType.stream()
                .filter( pt -> authz.checkIfHasPermissions( Arrays.asList( entitySet.getAclKey(),
                        pt.getAclKey() ),
                        Principals.getCurrentPrincipals(),
                        EnumSet.of( Permission.WRITE ) ) )
                .map( PropertyType::getType )
                .collect( Collectors.toSet() );
    }
}
