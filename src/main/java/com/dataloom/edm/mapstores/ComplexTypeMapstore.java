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

import com.dataloom.edm.type.ComplexType;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;

public class ComplexTypeMapstore extends AbstractStructuredCassandraPartitionKeyValueStore<UUID, ComplexType> {
    private static final CassandraTableBuilder ctb = Table.COMPLEX_TYPES.getBuilder();

    public ComplexTypeMapstore( Session session ) {
        super( HazelcastMap.COMPLEX_TYPES.name(), session, ctb );
    }

    @Override
    protected BoundStatement bind( UUID key, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ID.cql(), key );
    }

    @Override
    protected BoundStatement bind( UUID key, ComplexType value, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ID.cql(), key )
                .setString( CommonColumns.NAMESPACE.cql(), value.getType().getNamespace() )
                .setString( CommonColumns.NAME.cql(), value.getType().getName() )
                .setString( CommonColumns.TITLE.cql(), value.getTitle() )
                .setString( CommonColumns.DESCRIPTION.cql(), value.getDescription() )
                .setSet( CommonColumns.PROPERTIES.cql(), value.getProperties(), UUID.class )
                .setSet( CommonColumns.SCHEMAS.cql(), value.getSchemas(), FullQualifiedName.class )
                .setUUID( CommonColumns.BASE_TYPE.cql(), value.getBaseType().orNull() );
    }

    @Override
    protected UUID mapKey( Row rs ) {
        return rs == null ? null : rs.getUUID( CommonColumns.ID.cql() );
    }

    @Override
    protected ComplexType mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return RowAdapters.complexType( row );
    }

    @Override
    public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override
    public ComplexType generateTestValue() {
        return TestDataFactory.complexType();
    }

}
