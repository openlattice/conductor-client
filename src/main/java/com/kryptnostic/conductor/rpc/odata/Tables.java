package com.kryptnostic.conductor.rpc.odata;

import static com.dataloom.edm.internal.DatastoreConstants.ENTITY_SETS_OWNER_LOOKUP_TABLE;
import static com.dataloom.edm.internal.DatastoreConstants.ENTITY_SETS_OWNER_TABLE;
import static com.dataloom.edm.internal.DatastoreConstants.ENTITY_SETS_ROLES_ACLS_TABLE;
import static com.dataloom.edm.internal.DatastoreConstants.ENTITY_SETS_TABLE;
import static com.dataloom.edm.internal.DatastoreConstants.ENTITY_SETS_USERS_ACLS_TABLE;
import static com.dataloom.edm.internal.DatastoreConstants.ENTITY_SET_MEMBERS_TABLE;
import static com.dataloom.edm.internal.DatastoreConstants.ENTITY_TYPES_ROLES_ACLS_TABLE;
import static com.dataloom.edm.internal.DatastoreConstants.ENTITY_TYPES_TABLE;
import static com.dataloom.edm.internal.DatastoreConstants.ENTITY_TYPES_USERS_ACLS_TABLE;
import static com.dataloom.edm.internal.DatastoreConstants.PROPERTY_TYPES_IN_ENTITY_SETS_ROLES_ACLS_TABLE;
import static com.dataloom.edm.internal.DatastoreConstants.PROPERTY_TYPES_IN_ENTITY_SETS_USERS_ACLS_TABLE;
import static com.dataloom.edm.internal.DatastoreConstants.PROPERTY_TYPES_IN_ENTITY_TYPES_ROLES_ACLS_TABLE;
import static com.dataloom.edm.internal.DatastoreConstants.PROPERTY_TYPES_IN_ENTITY_TYPES_USERS_ACLS_TABLE;
import static com.dataloom.edm.internal.DatastoreConstants.PROPERTY_TYPES_TABLE;
import static com.dataloom.edm.internal.DatastoreConstants.ROLES_ACLS_REQUESTS_LOOKUP_TABLE;
import static com.dataloom.edm.internal.DatastoreConstants.ROLES_ACLS_REQUESTS_TABLE;
import static com.dataloom.edm.internal.DatastoreConstants.SCHEMAS_ACLS_TABLE;
import static com.dataloom.edm.internal.DatastoreConstants.SCHEMAS_TABLE;
import static com.dataloom.edm.internal.DatastoreConstants.USERS_ACLS_REQUESTS_LOOKUP_TABLE;
import static com.dataloom.edm.internal.DatastoreConstants.USERS_ACLS_REQUESTS_TABLE;

import com.dataloom.edm.internal.DatastoreConstants;
import com.kryptnostic.rhizome.cassandra.TableDef;

public enum Tables implements TableDef {
    DATA( "data" ),
    ENTITY_ID_LOOKUP( "data_entity_id_lookup" ),
    ENTITIES( "entities" ),
    ENTITY_SETS( ENTITY_SETS_TABLE ),
    ENTITY_TYPES( ENTITY_TYPES_TABLE ),
    PROPERTY_TYPE_LOOKUP( "property_type_lookup" ),
    ENTITY_TYPE_LOOKUP( "entity_type_lookup" ),
    ENTITY_SET_LOOKUP( "entity_set_lookup" ),
    PROPERTIES( "_properties" ),
    PROPERTY_TYPES( PROPERTY_TYPES_TABLE ),
    ENTITY_ID_TO_TYPE( "entity_id_to_type" ),
    ENTITY_SET_MEMBERS( ENTITY_SET_MEMBERS_TABLE ),
    ENTITY_TYPES_ROLES_ACLS( ENTITY_TYPES_ROLES_ACLS_TABLE ),
    ENTITY_TYPES_USERS_ACLS( ENTITY_TYPES_USERS_ACLS_TABLE ),
    ENTITY_SETS_ROLES_ACLS( ENTITY_SETS_ROLES_ACLS_TABLE ),
    ENTITY_SETS_USERS_ACLS( ENTITY_SETS_USERS_ACLS_TABLE ),
    ENTITY_SETS_OWNER( ENTITY_SETS_OWNER_TABLE ),
    ENTITY_SETS_OWNER_LOOKUP( ENTITY_SETS_OWNER_LOOKUP_TABLE ),
    PROPERTY_TYPES_IN_ENTITY_TYPES_ROLES_ACLS( PROPERTY_TYPES_IN_ENTITY_TYPES_ROLES_ACLS_TABLE ),
    PROPERTY_TYPES_IN_ENTITY_TYPES_USERS_ACLS( PROPERTY_TYPES_IN_ENTITY_TYPES_USERS_ACLS_TABLE ),
    PROPERTY_TYPES_IN_ENTITY_SETS_ROLES_ACLS( PROPERTY_TYPES_IN_ENTITY_SETS_ROLES_ACLS_TABLE ),
    PROPERTY_TYPES_IN_ENTITY_SETS_USERS_ACLS( PROPERTY_TYPES_IN_ENTITY_SETS_USERS_ACLS_TABLE ),
    SCHEMAS( SCHEMAS_TABLE ),
    SCHEMAS_ACLS( SCHEMAS_ACLS_TABLE ),
    ROLES_ACLS_REQUESTS( ROLES_ACLS_REQUESTS_TABLE ),
    ROLES_ACLS_REQUESTS_LOOKUP( ROLES_ACLS_REQUESTS_LOOKUP_TABLE ),
    USERS_ACLS_REQUESTS( USERS_ACLS_REQUESTS_TABLE ),
    USERS_ACLS_REQUESTS_LOOKUP( USERS_ACLS_REQUESTS_LOOKUP_TABLE ),
    TYPENAMES( "typenames" ),
    FQNS( "fqns" ),
    FQN_ACL_KEY( "fqn_acl_key" ),
    ORGANIZATIONS( "organizations" );
    private static final String KEYSPACE = DatastoreConstants.KEYSPACE;
    private final String        tableName;

    private Tables( String tableName ) {
        this.tableName = tableName;
    }

    public String getName() {
        return tableName;
    }

    public String getKeyspace() {
        return KEYSPACE;
    }
}
