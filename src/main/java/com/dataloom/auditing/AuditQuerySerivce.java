package com.dataloom.auditing;

import com.dataloom.auditing.util.AuditUtil;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.authorization.SecurableObjectType;
import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.kryptnostic.conductor.codecs.EnumSetTypeCodec;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import org.springframework.scheduling.annotation.Scheduled;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Stream;

import static com.kryptnostic.datastore.cassandra.CommonColumns.*;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class AuditQuerySerivce {
    private static final byte[] RESERVED = "Reserved for future use." .getBytes( Charsets.UTF_8 );
    private final Session           session;
    private final PreparedStatement storeEvent;
    private final PreparedStatement top100;
    private final PreparedStatement clearTail;

    public AuditQuerySerivce( String keyspace, Session session ) {
        this.session = session;
        storeEvent = session.prepare( Tables.AUDIT_EVENTS.getBuilder().buildStoreQuery());
        top100 = session.prepare( top100( keyspace ) );
        clearTail = session.prepare( clearTail( keyspace ) );
    }

    public static Select top100( String keyspace ) {
        return QueryBuilder.select( CommonColumns.COUNT.cql(), CommonColumns.ACL_KEYS.cql() )
                .from( keyspace, Tables.AUDIT_METRICS.getName() )
                .where( CommonColumns.ACL_KEYS.eq() ).limit( 100 );
    }

    public static Delete.Where clearTail( String keyspace ) {
        return QueryBuilder.delete()
                .from( keyspace, Tables.AUDIT_METRICS.getName() )
                .where( CommonColumns.ACL_KEYS.eq() )
                .and( QueryBuilder.lt( CommonColumns.COUNT.cql(), CommonColumns.COUNT.bindMarker() ) );
    }

    private static BoundStatement bindToEmptyListPartition( BoundStatement bs ) {
        return bs.setList( CommonColumns.ACL_KEYS.cql(), ImmutableList.of(), UUID.class );
    }

    public void storeAuditableEvent( AuditableEvent event ) {
        Principal p = event.getPrincipal();
        BoundStatement s = storeEvent.bind()
                .setList( ACL_KEYS.cql(), event.getAclKey(), UUID.class )
                .setUUID( TIME_ID.cql(), event.getUuidTimestamp() )
                .set( PRINCIPAL_TYPE.cql(), p.getType(), PrincipalType.class )
                .setString( PRINCIPAL_ID.cql(), p.getId() )
                .set( PERMISSIONS.cql(), event.getEventType(), EnumSetTypeCodec.getTypeTokenForEnumSetPermission() )
                .setString( AUDIT_EVENT_DETAILS.cql(), event.getEventDetails() )
                .setBytes( BLOCK.cql(), ByteBuffer.wrap( RESERVED ) );
        session.executeAsync( s );
    }

    public Stream<AuditMetric> getTop100() {
        return Stream.of( top100.bind() )
                .map( AuditQuerySerivce::bindToEmptyListPartition )
                .map( session::execute )
                .flatMap( StreamUtil::stream )
                .map( AuditUtil::auditMetric );
    }

    @Scheduled( fixedRate = 30000 )
    public void clearLeaderboard() {
        java.util.Optional<AuditMetric> maybeAuditMetric = getTop100().min( AuditMetric::compareTo );
        maybeAuditMetric.ifPresent( m -> session
                .executeAsync( bindToEmptyListPartition( clearTail.bind() )
                        .setLong( CommonColumns.COUNT.cql(), m.getCounter() ) ) );

    }

}
