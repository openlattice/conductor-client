/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.dataloom.auditing.mapstores;

import com.auth0.jwt.internal.org.apache.commons.lang3.RandomUtils;
import com.dataloom.auditing.AuditMetric;
import com.dataloom.auditing.util.AuditUtil;
import com.dataloom.authorization.AclKey;
import com.dataloom.authorization.util.AuthorizationUtils;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.hazelcast.config.MapStoreConfig;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
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
        super( HazelcastMap.AUDIT_METRICS.name(), session, Table.AUDIT_METRICS.getBuilder() );
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
        return AclKey.wrap( AuthorizationUtils.aclKey( row ) );
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
        return QueryBuilder.insertInto( keyspace, Table.AUDIT_METRICS.getName() )
                .value( CommonColumns.ACL_KEYS.cql(), ImmutableList.of() )
                .value( CommonColumns.COUNT.cql(), CommonColumns.COUNT.bindMarker() )
                .value( CommonColumns.ACL_KEY_VALUE.cql(), CommonColumns.ACL_KEY_VALUE.bindMarker() );

    }
}
