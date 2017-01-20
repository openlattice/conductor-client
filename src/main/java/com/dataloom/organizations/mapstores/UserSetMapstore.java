package com.dataloom.organizations.mapstores;

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.RandomStringUtils;

import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.organizations.PrincipalSet;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.rhizome.cassandra.ColumnDef;

public class UserSetMapstore extends PrincipalSetMapstore {

    public UserSetMapstore( HazelcastMap map, Session session, Tables table, ColumnDef keyCol, ColumnDef valueCol ) {
        super( map, session, table, keyCol, valueCol );
    }

    @Override
    protected PrincipalSet mapValue( ResultSet rs ) {
        Row r = rs.one();
        if ( r == null ) {
            return null;
        }
        Set<String> users = r.getSet( valueCol.cql(), String.class );
        return PrincipalSet.wrap( users.stream().map( user -> new Principal( PrincipalType.USER, user ) ).collect( Collectors.toSet() ) );
    }

    @Override
    public PrincipalSet generateTestValue() {
        return PrincipalSet.wrap( ImmutableSet.of( new Principal( PrincipalType.USER, RandomStringUtils.randomAlphanumeric( 5 ) ),
                new Principal( PrincipalType.USER, RandomStringUtils.randomAlphanumeric( 5 ) ),
                new Principal( PrincipalType.USER, RandomStringUtils.randomAlphanumeric( 5 ) ) ) );
    }

}
