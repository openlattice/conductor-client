/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.dataloom.edm.schemas.mapstores;

import java.util.Set;

import org.apache.commons.lang3.RandomStringUtils;

import com.dataloom.hazelcast.HazelcastMap;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedStringSet;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;

public class SchemaMapstore extends AbstractStructuredCassandraMapstore<String, DelegatedStringSet> {
    private static final CassandraTableBuilder ctb = Tables.SCHEMAS.getBuilder();

    public SchemaMapstore( Session session ) {
        super( HazelcastMap.SCHEMAS.name(), session, ctb );
    }

    @Override
    protected BoundStatement bind( String key, BoundStatement bs ) {
        return bs.setString( CommonColumns.NAMESPACE.cql(), key );
    }

    @Override
    protected BoundStatement bind( String key, DelegatedStringSet value, BoundStatement bs ) {
        return bs.setString( CommonColumns.NAMESPACE.cql(), key )
                .setSet( CommonColumns.NAME_SET.cql(), value, String.class );
    }

    @Override
    protected String mapKey( Row rs ) {
        return rs == null ? null : rs.getString( CommonColumns.NAMESPACE.cql() );
    }

    @Override
    protected DelegatedStringSet mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return DelegatedStringSet.wrap( row.getSet( CommonColumns.NAME_SET.cql(), String.class ) );
    }

    @Override
    public String generateTestKey() {
        return RandomStringUtils.randomAlphanumeric( 5 );
    }

    @Override
    public DelegatedStringSet generateTestValue() {
        return DelegatedStringSet.wrap( ImmutableSet.of( RandomStringUtils.randomAlphanumeric( 5 ), RandomStringUtils.randomAlphanumeric( 5 ) ) );
    }

}
