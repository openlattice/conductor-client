package com.dataloom.organizations.mapstores;

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.RandomStringUtils;

import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.hazelcast.HazelcastMap;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.rhizome.cassandra.ColumnDef;

public class RoleSetMapstore extends PrincipalSetMapstore {

    public RoleSetMapstore( HazelcastMap map, Session session, Tables table, ColumnDef keyCol, ColumnDef valueCol ) {
        super( map, session, table, keyCol, valueCol );
    }

    @Override
    protected Set<Principal> mapValue( ResultSet rs ) {
        Row r = rs.one();
        if ( r == null ) {
            return null;
        }
        Set<String> roles = r.getSet( valueCol.cql(), String.class );
        return roles.stream().map( role -> new Principal( PrincipalType.ROLE, role ) ).collect( Collectors.toSet() );
    }

    @Override
    public Set<Principal> generateTestValue() {
        return ImmutableSet.of( new Principal( PrincipalType.ROLE, RandomStringUtils.randomAlphanumeric( 5 ) ),
                new Principal( PrincipalType.ROLE, RandomStringUtils.randomAlphanumeric( 5 ) ),
                new Principal( PrincipalType.ROLE, RandomStringUtils.randomAlphanumeric( 5 ) ) );
    }

}
