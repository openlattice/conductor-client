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

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.edm.PropertyType;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;

public class PropertyTypeMapstore extends AbstractStructuredCassandraPartitionKeyValueStore<UUID, PropertyType> {
    private static final CassandraTableBuilder ctb = Tables.PROPERTY_TYPES.getBuilder();

    public PropertyTypeMapstore( Session session ) {
        super( HazelcastMap.PROPERTY_TYPES.name(), session, ctb );
    }

    @Override
    protected BoundStatement bind( UUID key, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ID.cql(), key );
    }

    @Override
    protected BoundStatement bind( UUID key, PropertyType value, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ID.cql(), key )
                .setString( CommonColumns.NAMESPACE.cql(), value.getType().getNamespace() )
                .setString( CommonColumns.NAME.cql(), value.getType().getName() )
                .setString( CommonColumns.TITLE.cql(), value.getTitle() )
                .setString( CommonColumns.DESCRIPTION.cql(), value.getDescription() )
                .setSet( CommonColumns.SCHEMAS.cql(), value.getSchemas(), FullQualifiedName.class )
                .set( CommonColumns.DATATYPE.cql(), value.getDatatype(), EdmPrimitiveTypeKind.class )
                .setBool( CommonColumns.PII_FIELD.cql(), value.isPIIfield() );
    }

    @Override
    protected UUID mapKey( Row rs ) {
        return rs == null ? null : rs.getUUID( CommonColumns.ID.cql() );
    }

    @Override
    protected PropertyType mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return new PropertyType(
                row.getUUID( CommonColumns.ID.cql() ),
                new FullQualifiedName(
                        row.getString( CommonColumns.NAMESPACE.cql() ),
                        row.getString( CommonColumns.NAME.cql() ) ),
                row.getString( CommonColumns.TITLE.cql() ),
                Optional.of( row.getString( CommonColumns.DESCRIPTION.cql() ) ),
                row.getSet( CommonColumns.SCHEMAS.cql(), FullQualifiedName.class ),
                row.get( CommonColumns.DATATYPE.cql(), EdmPrimitiveTypeKind.class ),
                Optional.of( row.getBool( CommonColumns.PII_FIELD.cql() ) ) );
    }

    @Override
    public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override
    public PropertyType generateTestValue() {
        return TestDataFactory.propertyType();
    }

}
