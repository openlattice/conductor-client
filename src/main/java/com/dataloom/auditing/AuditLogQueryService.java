package com.dataloom.auditing;

import com.dataloom.neuron.AuditableSignal;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.kryptnostic.conductor.rpc.odata.Table;

public class AuditLogQueryService {

    private final Session           session;
    private final PreparedStatement storeQuery;

    public AuditLogQueryService( String keyspace, Session session ) {

        this.session = session;
        this.storeQuery = session.prepare( Table.AUDIT_LOG.getBuilder().buildStoreQuery() );
    }

    public void store( AuditableSignal signal ) {

        throw new UnsupportedOperationException( "NOT IMPLEMENTED!" );
    }
}
