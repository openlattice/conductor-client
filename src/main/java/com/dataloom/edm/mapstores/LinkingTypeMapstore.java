package com.dataloom.edm.mapstores;

import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.spark_project.guava.collect.Sets;

import com.dataloom.edm.type.LinkingType;
import com.dataloom.hazelcast.HazelcastMap;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;

public class LinkingTypeMapstore extends AbstractStructuredCassandraPartitionKeyValueStore<UUID, LinkingType> {
    private static final CassandraTableBuilder ctb = Table.LINKING_TYPES.getBuilder();

    public LinkingTypeMapstore( Session session ) {
        super( HazelcastMap.LINKING_TYPES.name(), session, ctb );
    }

    @Override
    protected BoundStatement bind( UUID key, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ID.cql(), key );
    }

    @Override
    protected BoundStatement bind( UUID key, LinkingType value, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ID.cql(), key )
                .setString( CommonColumns.NAMESPACE.cql(), value.getType().getNamespace() )
                .setString( CommonColumns.NAME.cql(), value.getType().getName() )
                .setString( CommonColumns.TITLE.cql(), value.getTitle() )
                .setString( CommonColumns.DESCRIPTION.cql(), value.getDescription() )
                .setSet( CommonColumns.SCHEMAS.cql(), value.getSchemas(), FullQualifiedName.class )
                .setUUID( CommonColumns.SRC.cql(), value.getSrc() )
                .setUUID( CommonColumns.DEST.cql(), value.getDest() )
                .setBool( CommonColumns.BIDIRECTIONAL.cql(), value.isBidirectional() );
    }

    @Override
    protected UUID mapKey( Row rs ) {
        return rs == null ? null : rs.getUUID( CommonColumns.ID.cql() );
    }

    @Override
    protected LinkingType mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return RowAdapters.linkingType( row );
    }

    @Override
    public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override
    public LinkingType generateTestValue() {
        return new LinkingType(
                Optional.of( UUID.randomUUID() ),
                new FullQualifiedName( "namespace", "name" ),
                "linking type title",
                Optional.of( "this is a linking type" ),
                Sets.newHashSet(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                false );
    }

}
