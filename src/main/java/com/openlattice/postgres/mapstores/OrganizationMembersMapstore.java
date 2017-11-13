package com.openlattice.postgres.mapstores;

import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.organizations.PrincipalSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang.RandomStringUtils;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.openlattice.postgres.PostgresColumn.ID;
import static com.openlattice.postgres.PostgresColumn.MEMBERS;
import static com.openlattice.postgres.PostgresTable.ORGANIZATIONS;

public class OrganizationMembersMapstore extends AbstractBasePostgresMapstore<UUID, PrincipalSet> {
    public OrganizationMembersMapstore( HikariDataSource hds ) {
        super( HazelcastMap.ORGANIZATIONS_MEMBERS.name(), ORGANIZATIONS, hds );
    }

    @Override protected List<PostgresColumnDefinition> keyColumns() {
        return ImmutableList.of( ID );
    }

    @Override
    protected List<PostgresColumnDefinition> valueColumns() {
        return ImmutableList.of( MEMBERS );
    }

    @Override
    protected void bind( PreparedStatement ps, UUID key, PrincipalSet value ) throws SQLException {
        ps.setObject( 1, key );

        Array principalArray = PostgresArrays
                .createTextArray( ps.getConnection(), value.stream().map( Principal::getId ) );
        ps.setArray( 2, principalArray );

        // UPDATE
        ps.setArray( 3, principalArray );
    }

    @Override protected void bind( PreparedStatement ps, UUID key ) throws SQLException {
        ps.setObject( 1, key );
    }

    @Override protected PrincipalSet mapToValue( ResultSet rs ) throws SQLException {
        Stream<String> users = Arrays.stream( (String[]) rs.getArray( MEMBERS.getName() ).getArray() );
        return PrincipalSet
                .wrap( users.map( user -> new Principal( PrincipalType.USER, user ) ).collect( Collectors.toSet() ) );
    }

    @Override protected UUID mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.id( rs );
    }

    @Override
    public Map<UUID, PrincipalSet> loadAll( Collection<UUID> keys ) {
        Map<UUID, PrincipalSet> result = Maps.newConcurrentMap();
        keys.parallelStream().forEach( id -> {
            PrincipalSet users = load(id);
            if ( users != null ) result.put( id, users );
        });
        return result;
    }

    @Override
    protected List<PostgresColumnDefinition> getInsertColumns() {
        return ImmutableList.of( ID, MEMBERS );
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public PrincipalSet generateTestValue() {
        return PrincipalSet
                .wrap( ImmutableSet.of( new Principal( PrincipalType.USER, RandomStringUtils.randomAlphanumeric( 5 ) ),
                        new Principal( PrincipalType.USER, RandomStringUtils.randomAlphanumeric( 5 ) ),
                        new Principal( PrincipalType.USER, RandomStringUtils.randomAlphanumeric( 5 ) ) ) );
    }
}
