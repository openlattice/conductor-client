package com.openlattice.postgres.mapstores;

import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedStringSet;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang.RandomStringUtils;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.openlattice.postgres.PostgresColumn.ALLOWED_EMAIL_DOMAINS;
import static com.openlattice.postgres.PostgresColumn.ID;
import static com.openlattice.postgres.PostgresTable.ORGANIZATIONS;

public class OrganizationEmailDomainsMapstore extends AbstractBasePostgresMapstore<UUID, DelegatedStringSet> {

    public OrganizationEmailDomainsMapstore( HikariDataSource hds ) {
        super( HazelcastMap.ALLOWED_EMAIL_DOMAINS.name(), ORGANIZATIONS, hds );
    }

    @Override protected List<PostgresColumnDefinition> keyColumns() {
        return ImmutableList.of( ID );
    }

    @Override protected List<PostgresColumnDefinition> valueColumns() {
        return ImmutableList.of( ALLOWED_EMAIL_DOMAINS );
    }

    @Override protected void bind(
            PreparedStatement ps, UUID key, DelegatedStringSet value ) throws SQLException {
        ps.setObject( 1, key );

        Array valueArr = PostgresArrays.createTextArray( ps.getConnection(), value.stream() );
        ps.setObject( 2, valueArr );

        // UPDATE
        ps.setObject( 3, valueArr );
    }

    @Override protected void bind( PreparedStatement ps, UUID key ) throws SQLException {
        ps.setObject( 1, key );
    }

    @Override protected DelegatedStringSet mapToValue( ResultSet rs ) throws SQLException {
        String[] value = (String[]) rs.getArray( ALLOWED_EMAIL_DOMAINS.getName() ).getArray();
        return DelegatedStringSet.wrap( Sets.newHashSet( value ) );
    }

    @Override protected UUID mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.id( rs );
    }

    @Override
    public Map<UUID, DelegatedStringSet> loadAll( Collection<UUID> keys ) {
        Map<UUID, DelegatedStringSet> result = Maps.newConcurrentMap();
        keys.parallelStream().forEach( id -> {
            DelegatedStringSet domains = load(id);
            if ( domains != null ) result.put( id, domains );
        });
        return result;
    }

    @Override
    protected List<PostgresColumnDefinition> getInsertColumns() {
        return ImmutableList.of( ID, ALLOWED_EMAIL_DOMAINS );
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public DelegatedStringSet generateTestValue() {
        return DelegatedStringSet.wrap( ImmutableSet.of( RandomStringUtils.random( 10 ),
                RandomStringUtils.random( 10 ),
                RandomStringUtils.random( 10 ) ) );
    }
}
