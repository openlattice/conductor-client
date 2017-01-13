package com.dataloom.hazelcast.pods;

import java.util.EnumSet;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.AclKeyPathFragment;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.mapstores.PermissionMapstore;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.edm.mapstores.AclKeysMapstore;
import com.dataloom.edm.mapstores.EntitySetMapstore;
import com.dataloom.edm.mapstores.EntityTypeMapstore;
import com.dataloom.edm.mapstores.FqnsMapstore;
import com.dataloom.edm.mapstores.PropertyTypeMapstore;
import com.datastax.driver.core.Session;
import com.kryptnostic.rhizome.mapstores.SelfRegisteringMapStore;
import com.kryptnostic.rhizome.pods.CassandraPod;

@Configuration
@Import( CassandraPod.class )
public class MapstoresPod {

    @Inject
    Session session;

    @Bean
    public SelfRegisteringMapStore<AceKey, EnumSet<Permission>> permissionMapstore() {
        return new PermissionMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, PropertyType> propertyTypeMapstore() {
        return new PropertyTypeMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, EntityType> entityTypeMapstore() {
        return new EntityTypeMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, EntitySet> entitySetMapstore() {
        return new EntitySetMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<FullQualifiedName, AclKeyPathFragment> aclKeysMapstore() {
        return new AclKeysMapstore( session );
    }

    @Bean
    public SelfRegisteringMapStore<AclKeyPathFragment, FullQualifiedName> fqnsMapstore() {
        return new FqnsMapstore( session );
    }

}
