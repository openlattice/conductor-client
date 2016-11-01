package com.kryptnostic.datastore.services;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.datastax.driver.core.Row;
import com.datastax.driver.mapping.Mapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.conductor.rpc.odata.Schema;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.services.requests.GetSchemasRequest.TypeDetails;
import com.kryptnostic.datastore.util.Util;

public class SchemaDetailsAdapter implements Function<Row, Schema> {
    private final SchemaFactory         schemaFactory;
    private final CassandraTableManager ctb;
    private final Mapper<EntityType>    entityTypeMapper;
    private final Mapper<PropertyType>  propertyTypeMapper;
    private final Set<TypeDetails>      requestedDetails;

    public SchemaDetailsAdapter(
            CassandraTableManager ctb,
            Mapper<EntityType> entityTypeMapper,
            Mapper<PropertyType> propertyTypeMapper,
            Set<TypeDetails> requestedDetails ) {
        this.ctb = ctb;
        this.entityTypeMapper = entityTypeMapper;
        this.propertyTypeMapper = propertyTypeMapper;
        this.requestedDetails = requestedDetails;
        this.schemaFactory = schemaFactoryWithAclId( ACLs.EVERYONE_ACL );
    }

    @Override
    public Schema apply( Row row ) {
        Schema schema = schemaFactory.fromRow( row );

        if ( schema != null ) {

            if ( requestedDetails.contains( TypeDetails.ENTITY_TYPES ) ) {
                addEntityTypesToSchema( schema );
            }

            if ( requestedDetails.contains( TypeDetails.PROPERTY_TYPES ) ) {
                addPropertyTypesToSchema( schema );
            }
        }

        return schema;
    }

    public void addEntityTypesToSchema( Schema schema ) {
        Set<EntityType> entityTypes = schema.getEntityTypeFqns().stream()
                .map( type -> entityTypeMapper.getAsync( type.getNamespace(), type.getName() ) )
                .map( futureEntityType -> Util.getFutureSafely( futureEntityType ) ).filter( e -> e != null )
                .collect( Collectors.toSet() );
        schema.addEntityTypes( entityTypes );
    }

    public void addPropertyTypesToSchema( Schema schema ) {
        Set<FullQualifiedName> propertyTypeNames = Sets.newHashSet();
        propertyTypeNames.addAll( schema.getPropertyTypeFqns() );

        if ( schema.getEntityTypes().isEmpty() && !schema.getEntityTypeFqns().isEmpty() ) {
            addEntityTypesToSchema( schema );
        }

        for ( EntityType entityType : schema.getEntityTypes() ) {
            propertyTypeNames.addAll( entityType.getProperties() );
        }

        Set<PropertyType> propertyTypes = propertyTypeNames.stream()
                .map( type -> propertyTypeMapper.getAsync( type.getNamespace(), type.getName() ) )
                .map( futurePropertyType -> Util.getFutureSafely( futurePropertyType ) ).filter( e -> e != null )
                .collect( Collectors.toSet() );

        schema.addPropertyTypes( propertyTypes );
    }

    private SchemaFactory schemaFactoryWithAclId( UUID aclId ) {
        return new SchemaFactory( aclId );
    }

    private static final class SchemaFactory {
        private final UUID aclId;

        SchemaFactory( UUID aclId ) {
            this.aclId = aclId;
        }

        public Schema fromRow( Row r ) {
            String namespace = r.getString( CommonColumns.NAMESPACE.cql() );
            String name = r.getString( CommonColumns.NAME.cql() );
            Set<FullQualifiedName> entityTypeFqns = r.get( CommonColumns.ENTITY_TYPES.cql(),
                    new TypeToken<Set<FullQualifiedName>>() {
                        private static final long serialVersionUID = 7226187471436343452L;
                    } );
            Set<FullQualifiedName> propertyTypeFqns = r.get( CommonColumns.PROPERTIES.cql(),
                    new TypeToken<Set<FullQualifiedName>>() {
                        private static final long serialVersionUID = 888512488865063571L;
                    } );
            if ( entityTypeFqns == null ) {
                entityTypeFqns = ImmutableSet.of();
            }
            if ( propertyTypeFqns == null ) {
                propertyTypeFqns = ImmutableSet.of();
            }
            return new Schema()
                    .setAclId( aclId )
                    .setNamespace( namespace )
                    .setName( name )
                    .setEntityTypeFqns( entityTypeFqns )
                    .setPropertyTypeFqns( propertyTypeFqns );

        }
    }

}
