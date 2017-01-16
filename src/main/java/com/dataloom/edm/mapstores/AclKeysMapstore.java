package com.dataloom.edm.mapstores;

import java.util.UUID;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.hazelcast.HazelcastMap;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;

public class AclKeysMapstore
        extends AbstractStructuredCassandraPartitionKeyValueStore<FullQualifiedName, UUID> {
    private static final CassandraTableBuilder ctb = Tables.ACL_KEYS.getBuilder();

    public AclKeysMapstore( Session session ) {
        super( HazelcastMap.ACL_KEYS.name(), session, ctb );
    }

    @Override
    protected BoundStatement bind( FullQualifiedName key, BoundStatement bs ) {
        return bs.set( CommonColumns.FQN.cql(), key, FullQualifiedName.class );
    }

    @Override
    protected BoundStatement bind( FullQualifiedName key, UUID value, BoundStatement bs ) {
        return bs
                .set( CommonColumns.FQN.cql(), key, FullQualifiedName.class )
                .setUUID( CommonColumns.SECURABLE_OBJECTID.cql(), value );
    }

    @Override
    protected FullQualifiedName mapKey( Row rs ) {
        return rs == null ? null : rs.get( CommonColumns.FQN.cql(), FullQualifiedName.class );
    }

    @Override
    protected UUID mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return row.getUUID( CommonColumns.SECURABLE_OBJECTID.cql() );
    }

    @Override
    public FullQualifiedName generateTestKey() {
        throw new NotImplementedException( "GENERATION OF TEST KEY NOT IMPLEMENTED FOR ACL KEY MAPSTORE." );
    }

    @Override
    public UUID generateTestValue() throws Exception {
        throw new NotImplementedException( "GENERATION OF TEST VALUE NOT IMPLEMENTED FOR ACL KEY MAPSTORE." );
    }

}
