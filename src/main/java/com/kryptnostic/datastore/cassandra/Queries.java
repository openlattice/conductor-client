package com.kryptnostic.datastore.cassandra;

import java.util.function.Function;

import com.datastax.driver.core.DataType;
import com.kryptnostic.conductor.rpc.odata.DatastoreConstants;
import com.kryptnostic.conductor.rpc.odata.Tables;

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
    }

    // Keyspace setup
    public static final String CREATE_KEYSPACE = "CREATE KEYSPACE IF NOT EXISTS sparks WITH REPLICATION={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND DURABLE_WRITES=true";

    // Table Creation
    public static final String getCreateSchemasTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.SCHEMAS )
                .ifNotExists()
                .partitionKey( CommonColumns.ACLID )
                .clusteringColumns( CommonColumns.NAMESPACE, CommonColumns.NAME )
                .columns( CommonColumns.ENTITY_TYPES )
                .buildQuery();
    }

    public static final String getCreateEntitySetsTable( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.ENTITY_SETS )
                .ifNotExists()
                .partitionKey( CommonColumns.TYPE )
                .clusteringColumns( CommonColumns.NAME )
                .columns( CommonColumns.TITLE )
                .buildQuery();
    }

    public static final String getCreateEntityTypesTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.ENTITY_TYPES )
                .ifNotExists()
                .partitionKey( CommonColumns.NAMESPACE )
                .clusteringColumns( CommonColumns.NAME )
                .columns( CommonColumns.TYPENAME, CommonColumns.KEY, CommonColumns.PROPERTIES )
                .buildQuery();
    }

    public static final String getCreatePropertyTypesTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.PROPERTY_TYPES )
                .ifNotExists()
                .partitionKey( CommonColumns.NAMESPACE )
                .clusteringColumns( CommonColumns.NAME )
                .columns( CommonColumns.TYPENAME, CommonColumns.DATATYPE, CommonColumns.MULTIPLICITY )
                .buildQuery();
    }

    public static final String getCreateEntityIdToTypenameTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.ENTITY_ID_TO_TYPE )
                .ifNotExists()
                .partitionKey( CommonColumns.OBJECTID )
                .clusteringColumns( CommonColumns.NAME )
                .columns( CommonColumns.TITLE )
                .buildQuery();
    }

    public static final String getCreateFqnLookupTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.FQN_LOOKUP )
                .ifNotExists()
                .partitionKey( CommonColumns.TYPENAME )
                .columns( CommonColumns.FQN )
                .buildQuery();
    }

    public static final String CREATE_PROPERTY_TABLE(
            String keyspace,
            String table,
            Function<CommonColumns, DataType> typeResolver ) {
        return new CassandraTableBuilder( keyspace, table )
                .ifNotExists()
                .partitionKey( CommonColumns.OBJECTID )
                .clusteringColumns( CommonColumns.VALUE )
                .columns( CommonColumns.SYNCIDS )
                .withTypeResolver( typeResolver )
                .buildQuery();
    }

    public static final String CREATE_ENTITY_TABLE(
            String keyspace,
            String table,
            Function<CommonColumns, DataType> typeResolver ) {
        return new CassandraTableBuilder( keyspace, table )
                .ifNotExists()
                .partitionKey( CommonColumns. OBJECTID )
                .clusteringColumns( CommonColumns.CLOCK )
                .columns( CommonColumns.ENTITYSETS, CommonColumns.SYNCIDS )
                .withTypeResolver( typeResolver )
                .buildQuery();
    }
    

    // Index creation
    /*
     * HOW DOES SOFTWARE EVEN WORK? https://issues.apache.org/jira/browse/CASSANDRA-11331 Need to remove specific index
     * name once we upgrade to version post patch.
     */
    public static final String CREATE_INDEX_ON_NAME                = "CREATE INDEX IF NOT EXISTS entity_sets_name_idx ON "
            + DatastoreConstants.KEYSPACE
            + "."
            + Tables.ENTITY_SETS.getTableName() + " (name)";
    /**
     * This is the query for adding the secondary index on the entitySets column for entity table of a given type
     */
    public static final String CREATE_INDEX_ON_ENTITY_ENTITY_SETS  = "CREATE INDEX IF NOT EXISTS ON %s.%s (entitysets)";

    // Lightweight transactions for object insertion.
    public static final String CREATE_SCHEMA_IF_NOT_EXISTS         = "INSERT INTO sparks."
            + Tables.SCHEMAS.getTableName()
            + " (namespace, name, aclId, entityTypeFqns) VALUES (?,?,?,?) IF NOT EXISTS";
    public static final String CREATE_ENTITY_SET_IF_NOT_EXISTS     = "INSERT INTO sparks."
            + DatastoreConstants.ENTITY_SETS_TABLE
            + " (type, name, title) VALUES (?,?,?) IF NOT EXISTS";
    public static final String CREATE_ENTITY_TYPE_IF_NOT_EXISTS    = "INSERT INTO sparks."
            + DatastoreConstants.ENTITY_TYPES_TABLE
            + " (namespace, name, typename, key, properties) VALUES (?,?,?,?,?) IF NOT EXISTS";
    public static final String CREATE_PROPERTY_TYPE_IF_NOT_EXISTS  = "INSERT INTO sparks."
            + DatastoreConstants.PROPERTY_TYPES_TABLE
            + " (namespace, name, typename, datatype, multiplicity) VALUES (?,?,?,?,?) IF NOT EXISTS";
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
    public static final String GET_ALL_PROPERTY_TYPES_IN_NAMESPACE = "select * from sparks."
            + DatastoreConstants.PROPERTY_TYPES_TABLE + " where namespace=:"
            + ParamNames.NAMESPACE;
    public static final String GET_ALL_SCHEMAS_IN_NAMESPACE        = "select * from sparks."
            + DatastoreConstants.SCHEMAS_TABLE
            + " where namespace =:" + ParamNames.NAMESPACE + " AND aclId IN :"
            + ParamNames.ACL_IDS + " ALLOW filtering";
    public static final String GET_ALL_NAMESPACES                  = "select * from sparks."
            + DatastoreConstants.SCHEMAS_TABLE
            + " where aclId IN :"
            + ParamNames.ACL_IDS + " ALLOW filtering";
    public static final String ADD_ENTITY_TYPES_TO_SCHEMA          = "UPDATE sparks."
            + DatastoreConstants.SCHEMAS_TABLE
            + " SET entityTypeFqns = entityTypeFqns + :"
            + ParamNames.ENTITY_TYPES + " where aclId = :" + ParamNames.ACL_ID + " AND namespace = :"
            + ParamNames.NAMESPACE + " AND name = :"
            + ParamNames.NAME;
    public static final String REMOVE_ENTITY_TYPES_FROM_SCHEMA     = "UPDATE sparks."
            + DatastoreConstants.SCHEMAS_TABLE
            + " SET entityTypeFqns = entityTypeFqns - :"
            + ParamNames.ENTITY_TYPES + " where aclId = :" + ParamNames.ACL_ID + " AND namespace = :"
            + ParamNames.NAMESPACE + " AND name = :"
            + ParamNames.NAME;
    public static final String COUNT_ENTITY_SET                    = "select count(*) from sparks."
            + DatastoreConstants.ENTITY_SETS_TABLE + " where type = ? AND name = ?";
}