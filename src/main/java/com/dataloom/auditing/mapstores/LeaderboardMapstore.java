package com.dataloom.auditing.mapstores;

import com.auth0.jwt.internal.org.apache.commons.lang3.RandomUtils;
import com.dataloom.auditing.AuditMetric;
import com.dataloom.auditing.util.AuditUtil;
import com.dataloom.authorization.AclKey;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.hazelcast.config.MapStoreConfig;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;

import java.util.UUID;

import static com.kryptnostic.datastore.cassandra.CommonColumns.*;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class LeaderboardMapstore extends AbstractStructuredCassandraPartitionKeyValueStore<AclKey, AuditMetric> {
    private static AclKey TEST_KEY = AclKey.wrap( TestDataFactory.aclKey() );
    private final PreparedStatement bookkeeper;

    public LeaderboardMapstore( String keyspace, Session session ) {
        super( HazelcastMap.AUDIT_METRICS.name(), session, Tables.AUDIT_METRICS.getBuilder() );
        bookkeeper = session.prepare( bookkeepingQuery( keyspace ) );
    }

    @Override public AclKey generateTestKey() {
        return TEST_KEY;
    }

    @Override public AuditMetric generateTestValue() {
        return new AuditMetric( RandomUtils.nextInt( 0, 1000 ), TEST_KEY );
    }

    @Override protected BoundStatement bind( AclKey key, BoundStatement bs ) {
        return bs.setList( ACL_KEYS.cql(), key.unwrap(), UUID.class );
    }

    @Override protected BoundStatement bind( AclKey key, AuditMetric value, BoundStatement bs ) {
        session.executeAsync( valueBind( value, bookkeeper.bind() ) );
        return valueBind( value, bind( key, bs ) );
    }

    @Override public MapStoreConfig getMapStoreConfig() {
        return super.getMapStoreConfig().setWriteDelaySeconds( 1 );
    }

    @Override protected AclKey mapKey( Row row ) {
        return AclKey.wrap( RowAdapters.aclKey( row ) );
    }

    @Override protected AuditMetric mapValue( ResultSet rs ) {
        Row row = rs.one();
        return row == null ? null : AuditUtil.auditMetric( row );
    }

    private BoundStatement valueBind( AuditMetric value, BoundStatement bs ) {
        return bs.setLong( COUNT.cql(), value.getCounter() )
                .setList( ACL_KEY_VALUE.cql(), value.getAclKey(), UUID.class );
    }

    public Insert bookkeepingQuery( String keyspace ) {
        return QueryBuilder.insertInto( keyspace, Tables.AUDIT_METRICS.getName() )
                .value( CommonColumns.ACL_KEYS.cql(), ImmutableList.of() )
                .value( CommonColumns.COUNT.cql(), CommonColumns.COUNT.bindMarker() )
                .value( CommonColumns.ACL_KEY_VALUE.cql(), CommonColumns.ACL_KEY_VALUE.bindMarker() );

    }
}
