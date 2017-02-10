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

package com.dataloom.edm.mapstores;

import java.util.UUID;

import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;

public class NamesMapstore
        extends AbstractStructuredCassandraPartitionKeyValueStore<UUID, String> {
    private static final CassandraTableBuilder ctb = Tables.NAMES.getBuilder();

    public NamesMapstore( Session session ) {
        super( HazelcastMap.NAMES.name(), session, ctb );
    }

    @Override
    protected BoundStatement bind( UUID key, BoundStatement bs ) {
        return bs
                .setUUID( CommonColumns.SECURABLE_OBJECTID.cql(), key );
    }

    @Override
    protected BoundStatement bind( UUID key, String value, BoundStatement bs ) {
        return bs
                .setUUID( CommonColumns.SECURABLE_OBJECTID.cql(), key )
                .setString( CommonColumns.NAME.cql(), value );
    }

    @Override
    protected UUID mapKey( Row rs ) {
        return rs == null ? null : rs.getUUID( CommonColumns.SECURABLE_OBJECTID.cql() );
    }

    @Override
    protected String mapValue( ResultSet rs ) {
        Row row = rs.one();
        return row == null ? null : row.getString( CommonColumns.NAME.cql() );
    }

    @Override
    public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override
    public String generateTestValue() {
        return TestDataFactory.name();
    }

}
