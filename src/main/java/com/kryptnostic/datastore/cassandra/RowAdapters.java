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

package com.kryptnostic.datastore.cassandra;

import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.Permission;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.data.EntityKey;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.Analyzer;
import com.dataloom.edm.type.ComplexType;
import com.dataloom.edm.type.EdgeType;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.EnumType;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.graph.core.objects.EdgeKey;
import com.dataloom.graph.core.objects.EdgeLabel;
import com.dataloom.graph.core.objects.GraphWrappedEdgeKey;
import com.dataloom.graph.core.objects.GraphWrappedEntityKey;
import com.dataloom.graph.core.objects.GraphWrappedVertexId;
import com.dataloom.graph.core.objects.LoomEdge;
import com.dataloom.graph.core.objects.LoomVertex;
import com.dataloom.graph.core.objects.VertexLabel;
import com.dataloom.organization.roles.OrganizationRole;
import com.dataloom.organization.roles.RoleKey;
import com.dataloom.requests.RequestStatus;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.reflect.TypeToken;
import com.kryptnostic.conductor.codecs.EnumSetTypeCodec;

public final class RowAdapters {
    static final Logger logger = LoggerFactory.getLogger( RowAdapters.class );

    private RowAdapters() {}

    public static SetMultimap<FullQualifiedName, Object> entity(
            ResultSet rs,
            Map<UUID, PropertyType> authorizedPropertyTypes,
            ObjectMapper mapper ) {
        final SetMultimap<FullQualifiedName, Object> m = HashMultimap.create();
        for ( Row row : rs ) {
            UUID propertyTypeId = row.getUUID( CommonColumns.PROPERTY_TYPE_ID.cql() );
            String entityId = row.getString( CommonColumns.ENTITYID.cql() );
            if ( propertyTypeId != null ) {
                PropertyType pt = authorizedPropertyTypes.get( propertyTypeId );
                m.put( pt.getType(),
                        CassandraSerDesFactory.deserializeValue( mapper,
                                row.getBytes( CommonColumns.PROPERTY_VALUE.cql() ),
                                pt.getDatatype(),
                                entityId ) );
            }
        }
        return m;
    }

    public static SetMultimap<UUID, Object> entityIndexedById(
            ResultSet rs,
            Map<UUID, PropertyType> authorizedPropertyTypes,
            ObjectMapper mapper ) {
        final SetMultimap<UUID, Object> m = HashMultimap.create();
        for ( Row row : rs ) {
            UUID propertyTypeId = row.getUUID( CommonColumns.PROPERTY_TYPE_ID.cql() );
            String entityId = row.getString( CommonColumns.ENTITYID.cql() );
            if ( propertyTypeId != null ) {
                PropertyType pt = authorizedPropertyTypes.get( propertyTypeId );
                m.put( propertyTypeId,
                        CassandraSerDesFactory.deserializeValue( mapper,
                                row.getBytes( CommonColumns.PROPERTY_VALUE.cql() ),
                                pt.getDatatype(),
                                entityId ) );
            }
        }
        return m;
    }

    public static Pair<SetMultimap<UUID, Object>, SetMultimap<FullQualifiedName, Object>> entityIdFQNPair(
            ResultSet rs,
            Map<UUID, PropertyType> authorizedPropertyTypes,
            ObjectMapper mapper ) {
        final SetMultimap<UUID, Object> mByUUID = HashMultimap.create();
        final SetMultimap<FullQualifiedName, Object> mByKey = HashMultimap.create();

        for ( Row row : rs ) {
            UUID propertyTypeId = row.getUUID( CommonColumns.PROPERTY_TYPE_ID.cql() );
            String entityId = row.getString( CommonColumns.ENTITYID.cql() );
            if ( propertyTypeId != null ) {
                PropertyType pt = authorizedPropertyTypes.get( propertyTypeId );
                Object value = CassandraSerDesFactory.deserializeValue( mapper,
                        row.getBytes( CommonColumns.PROPERTY_VALUE.cql() ),
                        pt.getDatatype(),
                        entityId );
                mByUUID.put( propertyTypeId,
                        value );
                mByKey.put( authorizedPropertyTypes.get( propertyTypeId ).getType(), value );
            }
        }
        return Pair.of( mByUUID, mByKey );
    }

    public static String entityId( Row row ) {
        return row.getString( CommonColumns.ENTITYID.cql() );
    }

    public static String name( Row row ) {
        return row.getString( CommonColumns.NAME.cql() );
    }

    public static String namespace( Row row ) {
        return row.getString( CommonColumns.NAMESPACE.cql() );
    }

    public static String title( Row row ) {
        return row.getString( CommonColumns.TITLE.cql() );
    }

    public static Optional<String> description( Row row ) {
        return Optional.fromNullable( row.getString( CommonColumns.DESCRIPTION.cql() ) );
    }

    public static Set<String> contacts( Row row ) {
        return row.getSet( CommonColumns.CONTACTS.cql(), String.class );
    }

    public static UUID id( Row row ) {
        return row.getUUID( CommonColumns.ID.cql() );
    }

    public static UUID entityTypeId( Row row ) {
        return row.getUUID( CommonColumns.ENTITY_TYPE_ID.cql() );
    }

    public static EntitySet entitySet( Row row ) {
        // TODO: Validate data read from Cassandra and log errors for invalid entries.
        UUID id = id( row );
        UUID entityTypeId = entityTypeId( row );
        String name = name( row );
        String title = title( row );
        Optional<String> description = description( row );
        Set<String> contacts = contacts( row );
        return new EntitySet( id, entityTypeId, name, title, description, contacts );
    }

    public static EnumType enumType( Row row ) {
        com.google.common.base.Optional<UUID> id = com.google.common.base.Optional.of( id( row ) );
        FullQualifiedName type = splitFqn( row );
        String title = title( row );
        Optional<String> description = description( row );
        Set<FullQualifiedName> schemas = schemas( row );
        LinkedHashSet<String> members = members( row );
        Optional<EdmPrimitiveTypeKind> dataType = Optional.fromNullable( primitveType( row ) );
        Optional<Boolean> piiField = pii( row );
        boolean flags = flags( row );
        Optional<Analyzer> maybeAnalyzer = analyzer( row );
        return new EnumType( id, type, title, description, members, schemas, dataType, flags, piiField, maybeAnalyzer );
    }

    public static PropertyType propertyType( Row row ) {
        UUID id = id( row );
        FullQualifiedName type = splitFqn( row );
        String title = title( row );
        Optional<String> description = description( row );
        Set<FullQualifiedName> schemas = schemas( row );
        EdmPrimitiveTypeKind dataType = primitveType( row );
        Optional<Boolean> piiField = pii( row );
        Optional<Analyzer> maybeAnalyzer = analyzer( row );
        return new PropertyType( id, type, title, description, schemas, dataType, piiField, maybeAnalyzer );
    }

    public static EntityType entityType( Row row ) {
        UUID id = id( row );
        FullQualifiedName type = new FullQualifiedName( namespace( row ), name( row ) );
        String title = title( row );
        Optional<String> description = description( row );
        Set<FullQualifiedName> schemas = row.getSet( CommonColumns.SCHEMAS.cql(), FullQualifiedName.class );
        LinkedHashSet<UUID> key = (LinkedHashSet<UUID>) row.getSet( CommonColumns.KEY.cql(), UUID.class );
        LinkedHashSet<UUID> properties = (LinkedHashSet<UUID>) row.getSet( CommonColumns.PROPERTIES.cql(), UUID.class );
        Optional<UUID> baseType = Optional.fromNullable( row.getUUID( CommonColumns.BASE_TYPE.cql() ) );
        final Optional<SecurableObjectType> category;
        String objectType = row.getString( CommonColumns.CATEGORY.cql() );
        if ( StringUtils.isBlank( objectType ) ) {
            category = Optional.of( SecurableObjectType.EntityType );
        } else {
            category = Optional.of( SecurableObjectType.valueOf( objectType ) );
        }
        return new EntityType( id, type, title, description, schemas, key, properties, baseType, category );
    }

    public static ComplexType complexType( Row row ) {
        UUID id = id( row );
        FullQualifiedName type = new FullQualifiedName( namespace( row ), name( row ) );
        String title = title( row );
        Optional<String> description = description( row );
        Set<FullQualifiedName> schemas = row.getSet( CommonColumns.SCHEMAS.cql(), FullQualifiedName.class );
        LinkedHashSet<UUID> properties = (LinkedHashSet<UUID>) row.getSet( CommonColumns.PROPERTIES.cql(), UUID.class );
        Optional<UUID> baseType = Optional.fromNullable( row.getUUID( CommonColumns.BASE_TYPE.cql() ) );
        SecurableObjectType category = SecurableObjectType.valueOf( row.getString( CommonColumns.CATEGORY.cql() ) );
        return new ComplexType( id, type, title, description, schemas, properties, baseType, category );
    }

    public static EdgeType edgeType( Row row ) {
        LinkedHashSet<UUID> src = (LinkedHashSet<UUID>) row.getSet( CommonColumns.SRC.cql(), UUID.class );
        LinkedHashSet<UUID> dest = (LinkedHashSet<UUID>) row.getSet( CommonColumns.DEST.cql(), UUID.class );
        boolean bidirectional = bidirectional( row );
        return new EdgeType( Optional.absent(), src, dest, bidirectional );
    }

    public static FullQualifiedName splitFqn( Row row ) {
        String namespace = row.getString( CommonColumns.NAMESPACE.cql() );
        String name = row.getString( CommonColumns.NAME.cql() );
        return new FullQualifiedName( namespace, name );
    }

    public static FullQualifiedName fqn( Row row ) {
        return row.get( CommonColumns.FQN.cql(), FullQualifiedName.class );
    }

    public static FullQualifiedName type( Row row ) {
        return row.get( CommonColumns.TYPE.cql(), FullQualifiedName.class );
    }

    public static SecurableObjectType securableObjectType( Row row ) {
        return row.get( CommonColumns.SECURABLE_OBJECT_TYPE.cql(), SecurableObjectType.class );
    }

    public static List<UUID> aclRoot( Row row ) {
        return row.getList( CommonColumns.ACL_ROOT.cql(), UUID.class );
    }

    public static Map<UUID, EnumSet<Permission>> aclChildrenPermissions( Row row ) {
        return row.getMap( CommonColumns.ACL_CHILDREN_PERMISSIONS.cql(),
                TypeToken.of( UUID.class ),
                EnumSetTypeCodec.getTypeTokenForEnumSetPermission() );
    }

    public static RequestStatus reqStatus( Row row ) {
        return row.get( CommonColumns.STATUS.cql(), RequestStatus.class );
    }

    public static String principalId( Row row ) {
        return row.getString( CommonColumns.PRINCIPAL_ID.cql() );
    }

    public static UUID requestId( Row row ) {
        return row.getUUID( CommonColumns.REQUESTID.cql() );
    }

    public static Set<EntityKey> entityKeys( Row row ) {
        return row.getSet( CommonColumns.ENTITY_KEYS.cql(), EntityKey.class );
    }

    public static Pair<UUID, Set<EntityKey>> linkedEntity( Row row ) {
        return Pair.of( row.getUUID( CommonColumns.VERTEX_ID.cql() ), entityKeys( row ) );
    }

    public static UUID syncId( Row row ) {
        return row.getUUID( CommonColumns.SYNCID.cql() );
    }

    public static UUID entitySetId( Row row ) {
        return row.getUUID( CommonColumns.ENTITY_SET_ID.cql() );
    }

    public static UUID organizationId( Row row ) {
        return row.getUUID( CommonColumns.ORGANIZATION_ID.cql() );
    }

    public static RoleKey roleKey( Row row ) {
        return new RoleKey( organizationId( row ), id( row ) );
    }

    public static OrganizationRole organizationRole( Row row ) {
        Optional<UUID> id = Optional.of( id( row ) );
        UUID organizationId = organizationId( row );
        String title = title( row );
        Optional<String> description = description( row );
        return new OrganizationRole( id, organizationId, title, description );
    }

    public static LinkedHashSet<String> members( Row row ) {
        return (LinkedHashSet<String>) row.getSet( CommonColumns.MEMBERS.cql(), String.class );
    }

    public static Set<FullQualifiedName> schemas( Row row ) {
        return row.getSet( CommonColumns.SCHEMAS.cql(), FullQualifiedName.class );
    }

    public static EdmPrimitiveTypeKind primitveType( Row row ) {
        return row.get( CommonColumns.DATATYPE.cql(), EdmPrimitiveTypeKind.class );
    }

    public static Optional<Analyzer> analyzer( Row row ) {
        return Optional.of( row.get( CommonColumns.ANALYZER.cql(), Analyzer.class ) );
    }

    public static Optional<Boolean> pii( Row row ) {
        return Optional.of( row.getBool( CommonColumns.PII_FIELD.cql() ) );
    }

    public static UUID src( Row row ) {
        return row.getUUID( CommonColumns.SRC.cql() );
    }

    public static UUID dest( Row row ) {
        return row.getUUID( CommonColumns.DEST.cql() );
    }

    public static boolean bidirectional( Row row ) {
        return row.getBool( CommonColumns.BIDIRECTIONAL.cql() );
    }

    private static boolean flags( Row row ) {
        return row.getBool( CommonColumns.FLAGS.cql() );
    }

    public static GraphWrappedVertexId graphWrappedVertexId( Row row ) {
        UUID graphId = row.getUUID( CommonColumns.GRAPH_ID.cql() );
        UUID vertexId = row.getUUID( CommonColumns.VERTEX_ID.cql() );
        return new GraphWrappedVertexId( graphId, vertexId );
    }

    public static GraphWrappedEntityKey graphWrappedEntityKey( Row row ) {
        UUID graphId = row.getUUID( CommonColumns.GRAPH_ID.cql() );
        EntityKey entityKey = row.get( CommonColumns.ENTITY_KEY.cql(), EntityKey.class );
        return new GraphWrappedEntityKey( graphId, entityKey );
    }

    public static EdgeKey edgeKey( Row row ) {
        UUID srcId = row.getUUID( CommonColumns.SRC_VERTEX_ID.cql() );
        UUID dstId = row.getUUID( CommonColumns.DST_VERTEX_ID.cql() );
        UUID timeId = row.getUUID( CommonColumns.TIME_UUID.cql() );
        return new EdgeKey( srcId, dstId, timeId );
    }

    public static EdgeLabel edgeLabel( Row row ) {
        UUID edgeType = row.getUUID( CommonColumns.EDGE_TYPE_ID.cql() );
        String edgeEntityId = row.getString( CommonColumns.EDGE_ENTITYID.cql() );
        EntityKey reference = new EntityKey( edgeType, edgeEntityId );

        UUID srcType = row.getUUID( CommonColumns.SRC_VERTEX_TYPE_ID.cql() );
        UUID dstType = row.getUUID( CommonColumns.DST_VERTEX_TYPE_ID.cql() );
        return new EdgeLabel( reference, srcType, dstType );
    }

    public static GraphWrappedEdgeKey graphWrappedEdgeKey( Row row ) {
        UUID graphId = row.getUUID( CommonColumns.GRAPH_ID.cql() );
        return new GraphWrappedEdgeKey( graphId, edgeKey( row ) );
    }

    public static LoomVertex loomVertex( Row row ) {
        UUID graphId = row.getUUID( CommonColumns.GRAPH_ID.cql() );
        UUID key = row.getUUID( CommonColumns.VERTEX_ID.cql() );
        VertexLabel label = new VertexLabel( row.get( CommonColumns.ENTITY_KEY.cql(), EntityKey.class ) );
        return new LoomVertex( graphId, key, label );
    }

    public static LoomEdge loomEdge( Row row ) {
        UUID graphId = row.getUUID( CommonColumns.GRAPH_ID.cql() );
        EdgeKey key = edgeKey( row );
        EdgeLabel label = edgeLabel( row );
        return new LoomEdge( graphId, key, label );
    }
}
