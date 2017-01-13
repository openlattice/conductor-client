package com.dataloom.edm.mapstores;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.authorization.AclKeyPathFragment;
import com.dataloom.authorization.SecurableObjectType;
import com.dataloom.hazelcast.HazelcastMap;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;

public class FqnsMapstore
        extends AbstractStructuredCassandraPartitionKeyValueStore<AclKeyPathFragment, FullQualifiedName> {
    private static final CassandraTableBuilder ctb = Tables.FQNS.getBuilder();

    public FqnsMapstore( Session session ) {
        super( HazelcastMap.FQNS.name(), session, ctb );
    }

    @Override
    protected BoundStatement bind( AclKeyPathFragment key, BoundStatement bs ) {
        return bs                .set( CommonColumns.SECURABLE_OBJECT_TYPE.cql(), key.getType(), SecurableObjectType.class )
                .setUUID( CommonColumns.SECURABLE_OBJECTID.cql(), key.getId() );
    }

    @Override
    protected BoundStatement bind( AclKeyPathFragment key, FullQualifiedName value, BoundStatement bs ) {
        return bs.set( CommonColumns.SECURABLE_OBJECT_TYPE.cql(), key.getType(), SecurableObjectType.class )
                .setUUID( CommonColumns.SECURABLE_OBJECTID.cql(), key.getId() )
                .set( CommonColumns.FQN.cql(), value, FullQualifiedName.class );
    }

    @Override
    protected AclKeyPathFragment mapKey( Row rs ) {
        return rs == null ? null : new AclKeyPathFragment( rs.get( CommonColumns.SECURABLE_OBJECT_TYPE.cql(), SecurableObjectType.class ),
                rs.getUUID( CommonColumns.SECURABLE_OBJECTID.cql() ) );
    }

    @Override
    protected FullQualifiedName mapValue( ResultSet rs ) {
        Row row = rs.one();
        return row == null ? null : row.get( CommonColumns.FQN.cql(), FullQualifiedName.class );
    }

    @Override
    public AclKeyPathFragment generateTestKey() {
        throw new NotImplementedException( "GENERATION OF TEST KEY NOT IMPLEMENTED FOR ACL KEY MAPSTORE." );
    }

    @Override
    public FullQualifiedName generateTestValue() throws Exception {
        throw new NotImplementedException( "GENERATION OF TEST VALUE NOT IMPLEMENTED FOR ACL KEY MAPSTORE." );
    }

}
