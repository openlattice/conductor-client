package com.dataloom.neuron.audit;

import java.util.UUID;

import com.dataloom.authorization.PrincipalType;
import com.dataloom.neuron.SignalType;
import com.dataloom.neuron.signals.AuditableSignal;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.rhizome.configuration.cassandra.CassandraConfiguration;

import static com.kryptnostic.datastore.cassandra.CommonColumns.ACL_KEYS;
import static com.kryptnostic.datastore.cassandra.CommonColumns.AUDIT_ID;
import static com.kryptnostic.datastore.cassandra.CommonColumns.BLOCK_ID;
import static com.kryptnostic.datastore.cassandra.CommonColumns.DATA_ID;
import static com.kryptnostic.datastore.cassandra.CommonColumns.EVENT_TYPE;
import static com.kryptnostic.datastore.cassandra.CommonColumns.PRINCIPAL_ID;
import static com.kryptnostic.datastore.cassandra.CommonColumns.PRINCIPAL_TYPE;
import static com.kryptnostic.datastore.cassandra.CommonColumns.TIME_UUID;

public class AuditLogQueryService {

    private final Session           session;
    private final PreparedStatement storeQuery;

    public AuditLogQueryService( CassandraConfiguration cassandraConfig, Session session ) {

        this.session = session;
        this.storeQuery = session.prepare( Table.AUDIT_LOG.getBuilder().buildStoreQuery() );
    }

    public void store( AuditableSignal signal ) {

        // TODO: how do I deal with Optionals?
        BoundStatement storeStatement = storeQuery.bind()
                .setList( ACL_KEYS.cql(), signal.getAclKey().get(), UUID.class )
                .set( EVENT_TYPE.cql(), signal.getType(), SignalType.class )
                .set( PRINCIPAL_TYPE.cql(), signal.getPrincipal().get().getType(), PrincipalType.class )
                .setString( PRINCIPAL_ID.cql(), signal.getPrincipal().get().getId() )
                .setUUID( AUDIT_ID.cql(), signal.getAuditId() )
                .setUUID( TIME_UUID.cql(), signal.getTimeId() )
                .setUUID( DATA_ID.cql(), signal.getEntityId().get() )
                .setUUID( BLOCK_ID.cql(), signal.getBlockId().get() );

        session.executeAsync( storeStatement );
    }
}
