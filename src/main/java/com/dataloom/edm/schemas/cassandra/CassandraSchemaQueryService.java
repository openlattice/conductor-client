package com.dataloom.edm.schemas.cassandra;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.edm.schemas.SchemaQueryService;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;

public class CassandraSchemaQueryService implements SchemaQueryService {
    private final Session           session;
    private final PreparedStatement propertyTypesInSchemaQuery;
    private final PreparedStatement entityTypesInSchemaQuery;

    public CassandraSchemaQueryService( String keyspace, Session session ) {
        this.session = checkNotNull( session, "Session cannot be null." );
        propertyTypesInSchemaQuery = session.prepare( getPropertyTypesInSchema( keyspace ) );
        entityTypesInSchemaQuery = session.prepare( getEntityTypesInSchema( keyspace ) );
    }

    private static RegularStatement getPropertyTypesInSchema( String keyspace ) {
        return QueryBuilder
                .select( CommonColumns.NAMESPACE.cql(), CommonColumns.NAME.cql() )
                .distinct()
                .from( keyspace, Tables.PROPERTY_TYPES.getName() )
                .where( QueryBuilder.contains( CommonColumns.SCHEMAS.cql(), CommonColumns.SCHEMAS.bindMarker() ) );
    }

    private static RegularStatement getEntityTypesInSchema( String keyspace ) {
        return QueryBuilder
                .select( CommonColumns.NAMESPACE.cql(), CommonColumns.NAME.cql() )
                .distinct()
                .from( keyspace, Tables.ENTITY_TYPES.getName() )
                .where( QueryBuilder.contains( CommonColumns.SCHEMAS.cql(), CommonColumns.SCHEMAS.bindMarker() ) );
    }

    /* (non-Javadoc)
     * @see com.dataloom.edm.schemas.cassandra.SchemaQueryService#getAllPropertyTypesInSchema(org.apache.olingo.commons.api.edm.FullQualifiedName)
     */
    @Override
    public Set<FullQualifiedName> getAllPropertyTypesInSchema( FullQualifiedName schemaName ) {
        ResultSet propertyTypes = session.execute(
                propertyTypesInSchemaQuery.bind()
                        .setString( CommonColumns.NAMESPACE.cql(), schemaName.getNamespace() )
                        .setString( CommonColumns.NAME.cql(), schemaName.getName() ) );
        return ImmutableSet.copyOf( Iterables.transform( propertyTypes, RowAdapters::splitFqn ) );
    }

    /* (non-Javadoc)
     * @see com.dataloom.edm.schemas.cassandra.SchemaQueryService#getAllEntityTypesInSchema(org.apache.olingo.commons.api.edm.FullQualifiedName)
     */
    @Override
    public Set<FullQualifiedName> getAllEntityTypesInSchema( FullQualifiedName schemaName ) {
        ResultSet propertyTypes = session.execute(
                entityTypesInSchemaQuery.bind()
                        .setString( CommonColumns.NAMESPACE.cql(), schemaName.getNamespace() )
                        .setString( CommonColumns.NAME.cql(), schemaName.getName() ) );
        return ImmutableSet.copyOf( Iterables.transform( propertyTypes, RowAdapters::splitFqn ) );
    }
}
