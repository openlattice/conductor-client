package com.kryptnostic.datastore.cassandra;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.edm.internal.DatastoreConstants;
import com.dataloom.edm.internal.PropertyType;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Preconditions;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder.ValueColumn;
import com.kryptnostic.rhizome.cassandra.ColumnDef;

public final class Queries {
    private Queries() {}

    public static final class ParamNames {
        public static final String ENTITY_TYPE  = "entType";
        public static final String ACL_IDS      = "aclIds";
        public static final String NAMESPACE    = "namespace";
        public static final String NAME         = "name";
        public static final String ENTITY_TYPES = "entTypes";
        public static final String ACL_ID       = "aId";
        public static final String OBJ_ID       = "objId";
        public static final String ENTITY_SETS  = "entSets";
        public static final String SYNC_IDS     = "sId";
        public static final String PROPERTIES   = "properties";
        public static final String KEY          = "key";
    }

    // Keyspace setup
    public static final String CREATE_KEYSPACE = "CREATE KEYSPACE IF NOT EXISTS sparks WITH REPLICATION={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND DURABLE_WRITES=true";

    // Table Creation
    public static final String createEntityTypesRolesAclsTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.ENTITY_TYPES_ROLES_ACLS )
                .ifNotExists()
                .partitionKey( CommonColumns.ROLE )
                .clusteringColumns( CommonColumns.ENTITY_TYPE )
                .columns( CommonColumns.PERMISSIONS )
                .buildQuery();
    }

    public static final String createEntityTypesUsersAclsTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.ENTITY_TYPES_USERS_ACLS )
                .ifNotExists()
                .partitionKey( CommonColumns.USER )
                .clusteringColumns( CommonColumns.ENTITY_TYPE )
                .columns( CommonColumns.PERMISSIONS )
                .buildQuery();
    }

    public static final String createEntitySetsRolesAclsTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.ENTITY_SETS_ROLES_ACLS )
                .ifNotExists()
                .partitionKey( CommonColumns.ROLE )
                .clusteringColumns( CommonColumns.ENTITY_SET )
                .columns( CommonColumns.PERMISSIONS )
                .buildQuery();
    }

    public static final String createEntitySetsUsersAclsTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.ENTITY_SETS_USERS_ACLS )
                .ifNotExists()
                .partitionKey( CommonColumns.USER )
                .clusteringColumns( CommonColumns.ENTITY_SET )
                .columns( CommonColumns.PERMISSIONS )
                .buildQuery();
    }

    public static final String createEntitySetsOwnerTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.ENTITY_SETS_OWNER )
                .ifNotExists()
                .partitionKey( CommonColumns.ENTITY_SET )
                .columns( CommonColumns.USER )
                .buildQuery();
    }

    public static final String createEntitySetsOwnerLookupTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.ENTITY_SETS_OWNER_LOOKUP )
                .ifNotExists()
                .partitionKey( CommonColumns.USER )
                .clusteringColumns( CommonColumns.ENTITY_SET )
                .buildQuery();
    }

    public static final String createSchemasAclsTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.SCHEMAS_ACLS )
                .ifNotExists()
                .partitionKey( CommonColumns.NAMESPACE, CommonColumns.NAME )
                .clusteringColumns( CommonColumns.ROLE )
                .columns( CommonColumns.PERMISSIONS )
                .buildQuery();
    }

    public static final String createPropertyTypesInEntityTypesRolesAclsTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_TYPES_ROLES_ACLS )
                .ifNotExists()
                .partitionKey( CommonColumns.ROLE )
                .clusteringColumns( CommonColumns.ENTITY_TYPE, CommonColumns.PROPERTY_TYPE )
                .columns( CommonColumns.PERMISSIONS )
                .buildQuery();
    }

    public static final String createPropertyTypesInEntityTypesUsersAclsTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_TYPES_USERS_ACLS )
                .ifNotExists()
                .partitionKey( CommonColumns.USER )
                .clusteringColumns( CommonColumns.ENTITY_TYPE, CommonColumns.PROPERTY_TYPE )
                .columns( CommonColumns.PERMISSIONS )
                .buildQuery();
    }

    public static final String createPropertyTypesInEntitySetsRolesAclsTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_SETS_ROLES_ACLS )
                .ifNotExists()
                .partitionKey( CommonColumns.ROLE )
                .clusteringColumns( CommonColumns.ENTITY_SET, CommonColumns.PROPERTY_TYPE )
                .columns( CommonColumns.PERMISSIONS )
                .buildQuery();
    }

    public static final String createPropertyTypesInEntitySetsUsersAclsTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.PROPERTY_TYPES_IN_ENTITY_SETS_USERS_ACLS )
                .ifNotExists()
                .partitionKey( CommonColumns.USER )
                .clusteringColumns( CommonColumns.ENTITY_SET, CommonColumns.PROPERTY_TYPE )
                .columns( CommonColumns.PERMISSIONS )
                .buildQuery();
    }

    public static final String createRolesAclsRequestsTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.ROLES_ACLS_REQUESTS )
                .ifNotExists()
                .partitionKey( CommonColumns.USER )
                .clusteringColumns( CommonColumns.ENTITY_SET, CommonColumns.CLOCK, CommonColumns.REQUESTID )
                .columns( CommonColumns.NAME, CommonColumns.PROPERTY_TYPE, CommonColumns.PERMISSIONS )
                .buildQuery();
    }

    public static final String createRolesAclsRequestsLookupTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.ROLES_ACLS_REQUESTS_LOOKUP )
                .ifNotExists()
                .partitionKey( CommonColumns.REQUESTID )
                .columns( CommonColumns.USER, CommonColumns.ENTITY_SET, CommonColumns.CLOCK )
                .buildQuery();
    }

    public static final String createUsersAclsRequestsTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.USERS_ACLS_REQUESTS )
                .ifNotExists()
                .partitionKey( CommonColumns.USER )
                .clusteringColumns( CommonColumns.ENTITY_SET, CommonColumns.CLOCK, CommonColumns.REQUESTID )
                .columns( CommonColumns.NAME, CommonColumns.PROPERTY_TYPE, CommonColumns.PERMISSIONS )
                .buildQuery();
    }

    public static final String createUsersAclsRequestsLookupTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.USERS_ACLS_REQUESTS_LOOKUP )
                .ifNotExists()
                .partitionKey( CommonColumns.REQUESTID )
                .columns( CommonColumns.USER, CommonColumns.ENTITY_SET, CommonColumns.CLOCK )
                .buildQuery();
    }

    public static final String createSchemasTableQuery( String keyspace, String table ) {
        return new CassandraTableBuilder( keyspace, table )
                .ifNotExists()
                .partitionKey( CommonColumns.NAMESPACE )
                .clusteringColumns( CommonColumns.NAME )
                .columns( CommonColumns.ENTITY_TYPES, CommonColumns.PROPERTIES )
                .buildQuery();
    }

    public static final String getCreateEntitySetsTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.ENTITY_SETS )
                .ifNotExists()
                .partitionKey( CommonColumns.TYPENAME )
                .clusteringColumns( CommonColumns.NAME )
                .columns( CommonColumns.TITLE )
                .buildQuery();
    }

    public static final String getCreateEntityTypesTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.ENTITY_TYPES )
                .ifNotExists()
                .partitionKey( CommonColumns.NAMESPACE )
                .clusteringColumns( CommonColumns.NAME )
                .columns( CommonColumns.TYPENAME, CommonColumns.KEY, CommonColumns.PROPERTIES, CommonColumns.SCHEMAS )
                .buildQuery();
    }

    public static final String getCreatePropertyTypesTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.PROPERTY_TYPES )
                .ifNotExists()
                .partitionKey( CommonColumns.NAMESPACE )
                .clusteringColumns( CommonColumns.NAME )
                .columns( CommonColumns.TYPENAME,
                        CommonColumns.DATATYPE,
                        CommonColumns.MULTIPLICITY,
                        CommonColumns.SCHEMAS )
                .buildQuery();
    }

    public static final String getCreatePropertyTypeLookupTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.PROPERTY_TYPE_LOOKUP )
                .ifNotExists()
                .partitionKey( CommonColumns.TYPENAME )
                .columns( CommonColumns.FQN )
                .buildQuery();
    }

    public static final String getCreateEntityTypeLookupTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.ENTITY_TYPE_LOOKUP )
                .ifNotExists()
                .partitionKey( CommonColumns.TYPENAME )
                .columns( CommonColumns.FQN )
                .buildQuery();
    }

    public static final String createPropertyTableQuery(
            String keyspace,
            String table,
            Function<ColumnDef, DataType> typeResolver ) {
        return new CassandraTableBuilder( keyspace, table )
                .ifNotExists()
                .partitionKey( CommonColumns.ENTITYID )
                .clusteringColumns( CommonColumns.VALUE )
                .columns( CommonColumns.SYNCIDS )
                .withTypeResolver( typeResolver )
                .buildQuery();
    }

    public static final String createEntityTable(
            String keyspace,
            String table,
            Map<FullQualifiedName, PropertyType> keyPropertyTypes,
            Collection<PropertyType> propertyTypes ) {

        Set<FullQualifiedName> kfqns = keyPropertyTypes.values().stream().map( pt -> pt.getFullQualifiedName() )
                .collect( Collectors.toSet() );
        Stream<PropertyType> streamPropertyTypes = keyPropertyTypes.values().stream();

        Stream<ValueColumn> streamClusteringValueColumns = streamPropertyTypes
                .map( pt -> new CassandraTableBuilder.ValueColumn(
                        fqnToColumnName( pt.getFullQualifiedName() ),
                        CassandraEdmMapping.getCassandraType( pt.getDatatype() ) ) );

        Stream<ValueColumn> streamValueColumns = propertyTypes.stream()
                .filter( e -> !kfqns.contains( e.getFullQualifiedName() ) )
                .map( svc -> new CassandraTableBuilder.ValueColumn(
                        fqnToColumnName( svc.getFullQualifiedName() ),
                        CassandraEdmMapping.getCassandraType( svc.getDatatype() ) ) );

        ValueColumn[] clusteringValueColumns = streamClusteringValueColumns.collect( Collectors.toSet() )
                .toArray( new CassandraTableBuilder.ValueColumn[ 0 ] );
        ValueColumn[] valueColumns = streamValueColumns.collect( Collectors.toSet() )
                .toArray( new CassandraTableBuilder.ValueColumn[ 0 ] );
        // List<ValueColumn> vcs = java.util.Arrays.asList( valueColumns ).stream()
        // .filter( vc -> vc.cql().contains( "key" ) ).collect( Collectors.toList() );
        //
        // List<ValueColumn> svcs = java.util.Arrays.asList( clusteringValueColumns ).stream()
        // .filter( vc -> vc.cql().contains( "key" ) ).collect( Collectors.toList() );
        // TODO: Decide if clock needs to be kept.
        return new CassandraTableBuilder( keyspace, table )
                .ifNotExists()
                .partitionKey( CommonColumns.ENTITYID )
                .clusteringColumns( CommonColumns.CLOCK )
                .clusteringColumns( clusteringValueColumns )
                .columns( CommonColumns.TYPENAME, CommonColumns.ENTITY_SETS, CommonColumns.SYNCIDS )
                .columns( valueColumns )
                .buildQuery();
    }

    public static String fqnToColumnName( FullQualifiedName fqn ) {
        Preconditions.checkState( !StringUtils.endsWith( fqn.getNamespace(), "_" ) );
        Preconditions.checkState( !StringUtils.startsWith( fqn.getName(), "_" ) );
        return StringUtils.replace( StringUtils.replace( fqn.getFullQualifiedNameAsString(), "_", "__" ), ".", "_" );
        // return fqn.getFullQualifiedNameAsString().replaceAll( "_", "__" ).replaceAll( ".", "_" );
    }

    public static String columnNameToFqn( FullQualifiedName fqn ) {
        return fqn.getFullQualifiedNameAsString().replaceAll( "__", "_" ).replaceAll( "_", "." );
    }

    public static final String createEntityTableIndex(
            String keyspace,
            String table,
            FullQualifiedName fqn ) {
        String query = "CREATE CUSTOM INDEX IF NOT EXISTS ON %s.%s (%s) USING 'org.apache.cassandra.index.sasi.SASIIndex'";
        return String.format( query, keyspace, table, fqnToColumnName( fqn ) );
    }

    public static final String getCreateEntityIdToTypenameTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.ENTITY_ID_TO_TYPE )
                .ifNotExists()
                .partitionKey( CommonColumns.ENTITYID )
                .columns( CommonColumns.TYPENAME )
                .buildQuery();
    }

    public static final String getCreateEntitySetMembersTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.ENTITY_SET_MEMBERS )
                .ifNotExists()
                .partitionKey( CommonColumns.TYPENAME, CommonColumns.NAME, CommonColumns.PARTITION_INDEX )
                .clusteringColumns( CommonColumns.ENTITYID )
                .buildQuery();
    }

    // Index creation
    /*
     * HOW DOES SOFTWARE EVEN WORK? https://issues.apache.org/jira/browse/CASSANDRA-11331 Need to remove specific index
     * name once we upgrade to version post patch.
     */
    public static final String CREATE_INDEX_ON_NAME               = "CREATE INDEX IF NOT EXISTS entity_sets_name_idx ON "
            + DatastoreConstants.KEYSPACE
            + "."
            + Tables.ENTITY_SETS.getName()
            + " (" + CommonColumns.NAME.cql() + ")";
    /**
     * This is the query for adding the secondary index on the entitySets column for entity table of a given type
     */
    public static final String CREATE_INDEX_ON_ENTITY_ENTITY_SETS = "CREATE INDEX IF NOT EXISTS ON %s.%s ("
            + CommonColumns.ENTITY_SETS.cql() + ")";

    public static String createIndex( String keyspace, String tableName, String columnName ) {
        return "CREATE INDEX IF NOT EXISTS ON "
                + keyspace
                + "."
                + tableName
                + " (" + columnName + ")";
    }

    public static String indexEntityTypeOnEntityTypesRolesAclsTableQuery( String keyspace ) {
        return createIndex( keyspace, Tables.ENTITY_TYPES_ROLES_ACLS.getName(), CommonColumns.ENTITY_TYPE.cql() );
    }

    public static String indexEntityTypeOnEntityTypesUsersAclsTableQuery( String keyspace ) {
        return createIndex( keyspace, Tables.ENTITY_TYPES_USERS_ACLS.getName(), CommonColumns.ENTITY_TYPE.cql() );
    }

    public static String indexEntitySetOnEntitySetsRolesAclsTableQuery( String keyspace ) {
        return createIndex( keyspace, Tables.ENTITY_SETS_ROLES_ACLS.getName(), CommonColumns.ENTITY_SET.cql() );
    }

    public static String indexEntitySetOnEntitySetsUsersAclsTableQuery( String keyspace ) {
        return createIndex( keyspace, Tables.ENTITY_SETS_USERS_ACLS.getName(), CommonColumns.ENTITY_SET.cql() );
    }

    public static String indexEntityTypeOnPropertyTypesInEntityTypesRolesAclsTableQuery( String keyspace ) {
        return createIndex( keyspace,
                Tables.PROPERTY_TYPES_IN_ENTITY_TYPES_ROLES_ACLS.getName(),
                CommonColumns.ENTITY_TYPE.cql() );
    }

    public static String indexEntityTypeOnPropertyTypesInEntityTypesUsersAclsTableQuery( String keyspace ) {
        return createIndex( keyspace,
                Tables.PROPERTY_TYPES_IN_ENTITY_TYPES_USERS_ACLS.getName(),
                CommonColumns.ENTITY_TYPE.cql() );
    }

    public static String indexEntitySetOnPropertyTypesInEntitySetsRolesAclsTableQuery( String keyspace ) {
        return createIndex( keyspace,
                Tables.PROPERTY_TYPES_IN_ENTITY_SETS_ROLES_ACLS.getName(),
                CommonColumns.ENTITY_SET.cql() );
    }

    public static String indexEntitySetOnPropertyTypesInEntitySetsUsersAclsTableQuery( String keyspace ) {
        return createIndex( keyspace,
                Tables.PROPERTY_TYPES_IN_ENTITY_SETS_USERS_ACLS.getName(),
                CommonColumns.ENTITY_SET.cql() );
    }

    public static String indexEntitySetOnRolesAclsRequestsTableQuery( String keyspace ) {
        return createIndex( keyspace, Tables.ROLES_ACLS_REQUESTS.getName(), CommonColumns.ENTITY_SET.cql() );
    }

    public static String indexEntitySetOnUsersAclsRequestsTableQuery( String keyspace ) {
        return createIndex( keyspace, Tables.USERS_ACLS_REQUESTS.getName(), CommonColumns.ENTITY_SET.cql() );
    }

    // Lightweight transactions for object insertion.
    public static final RegularStatement createSchemaIfNotExists( String keyspace, String table ) {
        return QueryBuilder.insertInto( keyspace, table )
                .ifNotExists()
                .value( CommonColumns.NAMESPACE.cql(), QueryBuilder.bindMarker() )
                .value( CommonColumns.NAME.cql(), QueryBuilder.bindMarker() )
                .value( CommonColumns.ENTITY_TYPES.cql(), QueryBuilder.bindMarker() );
    }

    public static final String CREATE_ENTITY_SET_IF_NOT_EXISTS     = "INSERT INTO sparks."
            + DatastoreConstants.ENTITY_SETS_TABLE
            + " (typename, name, title) VALUES (?,?,?) IF NOT EXISTS";

    public static final String CREATE_ENTITY_TYPE_IF_NOT_EXISTS    = "INSERT INTO sparks."
            + DatastoreConstants.ENTITY_TYPES_TABLE
            + " (namespace, name, typename, key, properties, schemas) VALUES (?,?,?,?,?,?) IF NOT EXISTS";
    public static final String CREATE_PROPERTY_TYPE_IF_NOT_EXISTS  = "INSERT INTO sparks."
            + DatastoreConstants.PROPERTY_TYPES_TABLE
            + " (namespace, name, typename, datatype, multiplicity, schemas) VALUES (?,?,?,?,?,?) IF NOT EXISTS";
    public static final String UPDATE_PROPERTY_TYPE_IF_EXISTS      = "UPDATE sparks."
            + DatastoreConstants.PROPERTY_TYPES_TABLE
            + " SET datatype = ?" + "," + "multiplicity = ?" + "," + "schemas = ?"
            + " WHERE namespace = ?" + " AND " + "name = ?";
    public static final String INSERT_ENTITY_CLAUSES               = " (objectId, aclId, clock, entitySets, syncIds) VALUES( :"
            + ParamNames.OBJ_ID + ", :"
            + ParamNames.ACL_ID + ", toTimestamp(now()), :"
            + ParamNames.ENTITY_SETS + ", :"
            + ParamNames.SYNC_IDS + " ) IF objectId!=:"
            + ParamNames.OBJ_ID;

    // Read queries for datastore.
    public static final String GET_ALL_ENTITY_SETS                 = "select * from sparks."
            + DatastoreConstants.ENTITY_SETS_TABLE;
    public static final String GET_ALL_ENTITY_SETS_FOR_ENTITY_TYPE = "select * from sparks."
            + DatastoreConstants.ENTITY_SETS_TABLE + " where typename = ?";
    public static final String GET_ENTITY_SET_BY_NAME              = "select * from sparks."
            + DatastoreConstants.ENTITY_SETS_TABLE + " where name = ?";
    public static final String GET_ALL_ENTITY_TYPES_QUERY          = "select * from sparks."
            + DatastoreConstants.ENTITY_TYPES_TABLE;
    public static final String GET_ALL_PROPERTY_TYPES_QUERY        = "select * from sparks."
            + DatastoreConstants.PROPERTY_TYPES_TABLE;
    public static final String GET_ALL_PROPERTY_TYPES_IN_NAMESPACE = "select * from sparks."
            + DatastoreConstants.PROPERTY_TYPES_TABLE + " where namespace=:"
            + ParamNames.NAMESPACE;

    // Update statements for datastore.
    public static final String UPDATE_EXISTING_ENTITY_TYPE         = "UPDATE sparks."
            + DatastoreConstants.ENTITY_TYPES_TABLE + " SET properties = :"
            + ParamNames.PROPERTIES + " , key = :"
            + ParamNames.KEY + " WHERE namespace =:"
            + ParamNames.NAMESPACE + " AND name =:"
            + ParamNames.NAME;

    public static RegularStatement insertSchemaQueryIfNotExists( String keyspace, String table ) {
        return baseInsertSchemaQuery( QueryBuilder
                .insertInto( keyspace, table )
                .ifNotExists() );
    }

    public static RegularStatement insertSchemaQuery( String keyspace, String table ) {
        return baseInsertSchemaQuery( QueryBuilder
                .insertInto( keyspace, table ) );
    }

    public static RegularStatement baseInsertSchemaQuery( Insert statement ) {
        return statement.value( CommonColumns.NAMESPACE.cql(), QueryBuilder.bindMarker() )
                .value( CommonColumns.NAME.cql(), QueryBuilder.bindMarker() )
                .value( CommonColumns.ENTITY_TYPES.cql(), QueryBuilder.bindMarker() )
                .value( CommonColumns.PROPERTIES.cql(), QueryBuilder.bindMarker() );
    }

    public static RegularStatement getAllSchemasQuery( String keyspace, String table ) {
        return QueryBuilder.select().all().from( keyspace, table );
    }

    public static final RegularStatement getAllSchemasInNamespaceQuery( String keyspace, String table ) {
        return QueryBuilder.select().all().from( keyspace, table )
                .where( QueryBuilder.eq( CommonColumns.NAMESPACE.cql(), QueryBuilder.bindMarker() ) );
    }

    public static RegularStatement getSchemaQuery( String keyspace, String table ) {
        return QueryBuilder.select().all().from( keyspace, table )
                .where( QueryBuilder.eq( CommonColumns.NAMESPACE.cql(), QueryBuilder.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.NAME.cql(), QueryBuilder.bindMarker() ) );
    }

    public static final RegularStatement addEntityTypesToSchema( String keyspace, String table ) {
        return QueryBuilder.update( keyspace, table )
                .with( QueryBuilder.addAll( CommonColumns.ENTITY_TYPES.cql(), QueryBuilder.bindMarker() ) )
                .and( QueryBuilder.addAll( CommonColumns.PROPERTIES.cql(), QueryBuilder.bindMarker() ) )
                .where( QueryBuilder.eq( CommonColumns.NAMESPACE.cql(), QueryBuilder.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.NAME.cql(), QueryBuilder.bindMarker() ) );
    }

    public static final RegularStatement removeEntityTypesToSchema( String keyspace, String table ) {
        return QueryBuilder.update( keyspace, table )
                .with( QueryBuilder.removeAll( CommonColumns.ENTITY_TYPES.cql(), QueryBuilder.bindMarker() ) )
                .where( QueryBuilder.eq( CommonColumns.NAMESPACE.cql(), QueryBuilder.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.NAME.cql(), QueryBuilder.bindMarker() ) );
    }

    public static final RegularStatement countEntitySets( String keyspace ) {
        return QueryBuilder.select().countAll().from( keyspace, Tables.ENTITY_SETS.getName() )
                .where( QueryBuilder.eq( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.NAME.cql(), QueryBuilder.bindMarker() ) );
    }

    public static final RegularStatement addPropertyTypesToSchema( String keyspace, String table ) {
        return QueryBuilder.update( keyspace, table )
                .with( QueryBuilder.addAll( CommonColumns.PROPERTIES.cql(), QueryBuilder.bindMarker() ) )
                .where( QueryBuilder.eq( CommonColumns.NAMESPACE.cql(), QueryBuilder.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.NAME.cql(), QueryBuilder.bindMarker() ) );
    }

    public static final RegularStatement removePropertyTypesFromSchema( String keyspace, String table ) {
        return QueryBuilder.update( keyspace, table )
                .with( QueryBuilder.removeAll( CommonColumns.PROPERTIES.cql(), QueryBuilder.bindMarker() ) )
                .where( QueryBuilder.eq( CommonColumns.NAMESPACE.cql(), QueryBuilder.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.NAME.cql(), QueryBuilder.bindMarker() ) );
    }

    public static final String addPropertyColumnsToEntityTable( String keyspace, String table, String propertiesWithType ){

        return new StringBuilder( "ALTER TABLE " )
                .append( keyspace )
                .append( "." )
                .append( table )
                .append( " ADD (" )
                .append( propertiesWithType )
                .append( ")" )
                .toString();
    }

    public static final String dropPropertyColumnsFromEntityTable( String keyspace, String table, String propertyColumnNames ){

        return new StringBuilder( "ALTER TABLE " )
                .append( keyspace )
                .append( "." )
                .append( table )
                .append( " DROP (" )
                .append( propertyColumnNames )
                .append( ")" )
                .toString();
    }
}