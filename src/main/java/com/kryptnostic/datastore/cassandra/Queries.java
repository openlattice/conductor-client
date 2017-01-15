package com.kryptnostic.datastore.cassandra;

import com.dataloom.edm.internal.DatastoreConstants;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;

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



    public static final String getCreateEntitySetsTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.ENTITY_SETS )
                .ifNotExists()
                .partitionKey( CommonColumns.ID )
                .clusteringColumns( CommonColumns.TYPE, CommonColumns.NAME )
                .columns( CommonColumns.ENTITY_TYPE_ID, CommonColumns.TITLE, CommonColumns.DESCRIPTION )
                .buildCreateTableQuery();
    }

    public static final String getCreateEntityTypesTableQuery( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.ENTITY_TYPES )
                .ifNotExists()
                .partitionKey( CommonColumns.ID )
                .clusteringColumns( CommonColumns.NAMESPACE, CommonColumns.NAME )
                .columns( CommonColumns.KEY, CommonColumns.PROPERTIES, CommonColumns.SCHEMAS )
                .buildCreateTableQuery();
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
            + DatastoreConstants.ENTITY_SETS_TABLE + " where type = ?";
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

//    public static final RegularStatement countEntitySets( String keyspace ) {
//        return QueryBuilder.select().countAll().from( keyspace, Tables.ENTITY_SETS.getName() )
//                .where( QueryBuilder.eq( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() ) )
//                .and( QueryBuilder.eq( CommonColumns.NAME.cql(), QueryBuilder.bindMarker() ) );
//    }

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

    public static final String addPropertyColumnsToEntityTable(
            String keyspace,
            String table,
            String propertiesWithType ) {

        return new StringBuilder( "ALTER TABLE " )
                .append( keyspace )
                .append( "." )
                .append( table )
                .append( " ADD (" )
                .append( propertiesWithType )
                .append( ")" )
                .toString();
    }

    public static final String dropPropertyColumnsFromEntityTable(
            String keyspace,
            String table,
            String propertyColumnNames ) {

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