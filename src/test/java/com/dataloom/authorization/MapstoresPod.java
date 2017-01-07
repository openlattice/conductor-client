package com.dataloom.authorization;

import java.util.EnumSet;

import javax.inject.Inject;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.dataloom.authorization.mapstores.PermissionMapstore;
import com.dataloom.authorization.requests.Permission;
import com.dataloom.edm.internal.DatastoreConstants;
import com.datastax.driver.core.Session;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.mapstores.SelfRegisteringMapStore;
import com.kryptnostic.rhizome.pods.CassandraPod;

@Configuration
@Import( CassandraPod.class )
public class MapstoresPod {

    @Inject
    Session session;

    @Bean
    public SelfRegisteringMapStore<AceKey, EnumSet<Permission>> permissionMapstore() {
        return new PermissionMapstore(
                session,
                new CassandraTableBuilder( DatastoreConstants.KEYSPACE, PermissionMapstore.MAP_NAME )
                        .ifNotExists()
                        .partitionKey( CommonColumns.ACL_KEYS )
                        .clusteringColumns( CommonColumns.PRINCIPAL_TYPE, CommonColumns.PRINCIPAL_ID )
                        .columns( CommonColumns.SECURABLE_OBJECT_TYPE, CommonColumns.PERMISSIONS ) );
    }
}
