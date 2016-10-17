package com.kryptnostic.datastore.cassandra;

import java.util.function.Function;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.kryptnostic.conductor.rpc.odata.DatastoreConstants;
import com.kryptnostic.conductor.rpc.odata.Tables;

public final class Queries {
    private Queries() {}

    public static final class ParamNames {
        public static final String ENTITY_TYPE        = "entType";
        public static final String ACL_IDS            = "aclIds";
        public static final String NAMESPACE          = "namespace";
        public static final String NAME               = "name";
        public static final String ENTITY_TYPES       = "entTypes";
        public static final String ACL_ID             = "aId";
        public static final String OBJ_ID             = "objId";
        public static final String ENTITY_SETS        = "entSets";
        public static final String SYNC_IDS           = "sId";
        public static final String PROPERTIES         = "properties";
        public static final String KEY                = "key"; 
    }

    // Keyspace setup
    public static final String CREATE_KEYSPACE = "CREATE KEYSPACE IF NOT EXISTS sparks WITH REPLICATION={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND DURABLE_WRITES=true";

    // Table Creation
    public static final String createPropertyTypesAclsTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.PROPERTY_TYPES_ACLS )
                .ifNotExists()
                .partitionKey( CommonColumns.NAMESPACE, CommonColumns.NAME )
                .clusteringColumns( CommonColumns.ACLID )
                .columns( CommonColumns.PERMISSIONS )
                .buildQuery();
    }
    
    public static final String createEntityTypesAclsTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.PROPERTY_TYPES_ACLS )
                .ifNotExists()
                .partitionKey( CommonColumns.NAMESPACE, CommonColumns.NAME )
                .clusteringColumns( CommonColumns.ACLID )
                .columns( CommonColumns.PERMISSIONS )
                .buildQuery();
    }
    
    public static final String createEntitySetsAclsTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.PROPERTY_TYPES_ACLS )
                .ifNotExists()
                .partitionKey( CommonColumns.NAME )
                .clusteringColumns( CommonColumns.ACLID )
                .columns( CommonColumns.PERMISSIONS )
                .buildQuery();
    }
    
    public static final String createSchemasAclsTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.PROPERTY_TYPES_ACLS )
                .ifNotExists()
                .partitionKey( CommonColumns.NAMESPACE, CommonColumns.NAME )
                .clusteringColumns( CommonColumns.ACLID )
                .columns( CommonColumns.PERMISSIONS )
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
                .columns( CommonColumns.TYPENAME, CommonColumns.DATATYPE, CommonColumns.MULTIPLICITY, CommonColumns.SCHEMAS )
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
            Function<CommonColumns, DataType> typeResolver ) {
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
            String table ) {
        return new CassandraTableBuilder( keyspace, table )
                .ifNotExists()
                .partitionKey( CommonColumns.ENTITYID )
                .clusteringColumns( CommonColumns.CLOCK )
                .columns( CommonColumns.TYPENAME, CommonColumns.ENTITYSETS, CommonColumns.SYNCIDS )
                .buildQuery();
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
            + Tables.ENTITY_SETS.getTableName() + " (name)";
    /**
     * This is the query for adding the secondary index on the entitySets column for entity table of a given type
     */
    public static final String CREATE_INDEX_ON_ENTITY_ENTITY_SETS = "CREATE INDEX IF NOT EXISTS ON %s.%s (entitysets)";

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
    public static final String UPDATE_PROPERTY_TYPE_IF_EXISTS = "UPDATE sparks."
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
	public static final String UPDATE_EXISTING_ENTITY_TYPE = "UPDATE sparks."
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
                .and( QueryBuilder.addAll(CommonColumns.PROPERTIES.cql(), QueryBuilder.bindMarker() ) )
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
        return QueryBuilder.select().countAll().from( keyspace, Tables.ENTITY_SETS.getTableName() )
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
}