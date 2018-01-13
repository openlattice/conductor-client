package com.openlattice.postgres;

import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.hazelcast.HazelcastMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.util.Util;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class SqlQueryFactory {
    private final IMap<UUID, EntitySet>    entitySets;
    private final IMap<UUID, PropertyType> propertyTypes;
    private final IMap<UUID, EntityType>   entityTypes;

    public SqlQueryFactory( HazelcastInstance hazelcastInstance ) {
        entitySets = hazelcastInstance.getMap( HazelcastMap.ENTITY_SETS.name() );
        propertyTypes = hazelcastInstance.getMap( HazelcastMap.PROPERTY_TYPES.name() );
        entityTypes = hazelcastInstance.getMap( HazelcastMap.ENTITY_TYPES.name() );
    }

    public String getEntitySetQuery( UUID entitySetId ) {
        EntitySet entitySet = Util.getSafely( entitySets, entitySetId );
        EntityType entityType = Util.getSafely( entityTypes, entitySet.getEntityTypeId() );
        List<PropertyType> propertyTypesForEntitySet = entityType.getProperties()
                .stream()
                .map( propertyTypes::get )
                .collect( Collectors.toList() );
        String idColumn = idColumn( entitySetId );

        String colSql = idColumn + ", " + propertyTypesForEntitySet
                .stream()
                .map( propertyType -> columnSql( entitySetId, propertyType.getId(), propertyType.getType() ) )
                .collect( Collectors.joining( ", " ) );
        String tableSql = propertyTypesForEntitySet
                .stream()
                .map( propertyType -> DataTables.propertyTableName( entitySetId, propertyType.getId() ) )
                .collect( Collectors.joining( ", " ) );
        String joinSql = propertyTypesForEntitySet
                .stream()
                .map( propertyType -> idColumn + " = " + idColumn( entitySetId, propertyType.getId() ) )
                .collect( Collectors.joining( " and " ) );
        return "select " + colSql + "from " + tableSql + "where " + joinSql;
    }

    private static String columnSql( UUID entitySetId, UUID propertyTypeId, FullQualifiedName fqn ) {
        return DataTables.propertyTableName( entitySetId, propertyTypeId ) + ".value as " + fqn
                .getFullQualifiedNameAsString();
    }

    private static String idColumn( UUID entitySetId, UUID propertyTypeId ) {
        return DataTables.propertyTableName( entitySetId, propertyTypeId ) + ".id";
    }

    private static String idColumn( UUID entitySetId ) {
        return DataTables.entityTableName( entitySetId ) + ".id";
    }

    private static String idColumnSql( UUID propertyTypeId, FullQualifiedName fqn ) {
        return idColumn( propertyTypeId ) + " as " + fqn.getFullQualifiedNameAsString();
    }

}