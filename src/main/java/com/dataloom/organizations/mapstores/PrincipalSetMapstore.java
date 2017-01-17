package com.dataloom.organizations.mapstores;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.dataloom.authorization.Principal;
import com.dataloom.hazelcast.HazelcastMap;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.rhizome.cassandra.ColumnDef;

public abstract class PrincipalSetMapstore extends UUIDKeyMapstore<Set<Principal>> {
    protected final ColumnDef valueCol;

    public PrincipalSetMapstore(
            HazelcastMap map,
            Session session,
            Tables table,
            ColumnDef keyCol,
            ColumnDef valueCol ) {
        super( map, session, table, keyCol );
        this.valueCol = valueCol;
    }

    @Override
    protected BoundStatement bind( UUID key, Set<Principal> value, BoundStatement bs ) {
        return bs
                .setUUID( keyCol.cql(), key )
                .setSet( valueCol.cql(),
                        value.stream().map( Principal::getId ).collect( Collectors.toSet() ),
                        String.class );
    }

}
