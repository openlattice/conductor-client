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

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.edm.EntityType;
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

public class EntityTypeMapstore extends AbstractStructuredCassandraPartitionKeyValueStore<UUID, EntityType> {
    private static final CassandraTableBuilder ctb = Tables.ENTITY_TYPES.getBuilder();

    public EntityTypeMapstore( Session session ) {
        super( HazelcastMap.ENTITY_TYPES.name(), session, ctb );
    }

    @Override
    protected BoundStatement bind( UUID key, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ID.cql(), key );
    }

    @Override
    protected BoundStatement bind( UUID key, EntityType value, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ID.cql(), key )
                .setString( CommonColumns.NAMESPACE.cql(), value.getType().getNamespace() )
                .setString( CommonColumns.NAME.cql(), value.getType().getName() )
                .setString( CommonColumns.TITLE.cql(), value.getTitle() )
                .setString( CommonColumns.DESCRIPTION.cql(), value.getDescription() )
                .setSet( CommonColumns.KEY.cql(), value.getKey(), UUID.class )
                .setSet( CommonColumns.PROPERTIES.cql(), value.getProperties(), UUID.class )
                .setSet( CommonColumns.SCHEMAS.cql(), value.getSchemas(), FullQualifiedName.class );
    }

    @Override
    protected UUID mapKey( Row rs ) {
        return rs == null ? null : rs.getUUID( CommonColumns.ID.cql() );
    }

    @Override
    protected EntityType mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return new EntityType(
                row.getUUID( CommonColumns.ID.cql() ),
                new FullQualifiedName(
                        row.getString( CommonColumns.NAMESPACE.cql() ),
                        row.getString( CommonColumns.NAME.cql() ) ),
                row.getString( CommonColumns.TITLE.cql() ),
                Optional.of( row.getString( CommonColumns.DESCRIPTION.cql() ) ),
                row.getSet( CommonColumns.SCHEMAS.cql(), FullQualifiedName.class ),
                row.getSet( CommonColumns.KEY.cql(), UUID.class ),
                row.getSet( CommonColumns.PROPERTIES.cql(), UUID.class ) );
    }

    @Override
    public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override
    public EntityType generateTestValue() {
        return TestDataFactory.entityType();
    }

}
