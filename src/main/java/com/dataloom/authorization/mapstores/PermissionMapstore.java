package com.dataloom.authorization.mapstores;

import java.util.Set;

import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.requests.Permission;
import com.dataloom.authorization.util.CassandraMappingUtils;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.kryptnostic.conductor.codecs.EnumSetTypeCodec;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;

public class PermissionMapstore extends AbstractStructuredCassandraMapstore<AceKey, Set<Permission>>{
    public static final String MAP_NAME = "authorizations";
    public PermissionMapstore(
            Session session,
            CassandraTableBuilder tableBuilder ) {
        super( MAP_NAME, session, tableBuilder );
    }

    @Override
    protected BoundStatement bind( AceKey key, BoundStatement bs ) {
        return bs.bind( key.getObjectType() , key.getObjectType() , key.getPrincipal().getType(), key.getPrincipal().getName() );
    }
    
    @Override
    protected BoundStatement bind( AceKey key, Set<Permission> permissions, BoundStatement bs ) {
        return bs.bind( key.getObjectType() , key.getObjectType() , key.getPrincipal().getType(), key.getPrincipal().getName() );
    }
    

    @Override
    protected AceKey mapKey( Row row ) {
        return CassandraMappingUtils.getAceKeyFromRow( row );
    }

    @Override
    protected Set<Permission> mapValue( Row row ) {
        return row.get( CommonColumns.PERMISSIONS.cql(), EnumSetTypeCodec.getTypeTokenForEnumSetPermission() );
    }

    @Override
    public AceKey generateTestKey() {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Set<Permission> generateTestValue() throws Exception {
        // TODO Auto-generated method stub
        return null;
    }
}
