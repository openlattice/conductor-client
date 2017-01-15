package com.dataloom.edm.mapstores;

import java.util.UUID;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.edm.internal.PropertyType;
import com.dataloom.hazelcast.HazelcastMap;
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
                .set( CommonColumns.DATATYPE.cql(), value.getDatatype(), EdmPrimitiveTypeKind.class );
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
                row.get( CommonColumns.DATATYPE.cql(), EdmPrimitiveTypeKind.class ) );
    }
    
    @Override
    public UUID generateTestKey() {
        throw new NotImplementedException( "GENERATION OF TEST KEY NOT IMPLEMENTED FOR PROPERTY TYPE MAPSTORE." );
    }

    @Override
    public PropertyType generateTestValue() throws Exception {
        throw new NotImplementedException( "GENERATION OF TEST VALUE NOT IMPLEMENTED FOR PROPERTY TYPE MAPSTORE." );
    }

    
}
