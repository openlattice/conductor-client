package com.dataloom.hazelcast;

import java.util.EnumSet;

import javax.inject.Inject;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.mapstores.PermissionMapstore;
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
}
