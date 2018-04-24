

/*
 * Copyright (C) 2018. OpenLattice, Inc.
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
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */

package com.openlattice.datastore.services;

import static com.google.common.base.Preconditions.checkState;

import com.dataloom.streams.StreamUtil;
import com.openlattice.edm.events.AssociationTypeCreatedEvent;
import com.openlattice.edm.events.AssociationTypeDeletedEvent;
import com.openlattice.edm.events.ClearAllDataEvent;
import com.openlattice.edm.events.EntitySetCreatedEvent;
import com.openlattice.edm.events.EntitySetDeletedEvent;
import com.openlattice.edm.events.EntitySetMetadataUpdatedEvent;
import com.openlattice.edm.events.EntityTypeCreatedEvent;
import com.openlattice.edm.events.EntityTypeDeletedEvent;
import com.openlattice.edm.events.PropertyTypeCreatedEvent;
import com.openlattice.edm.events.PropertyTypeDeletedEvent;
import com.openlattice.edm.events.PropertyTypesInEntitySetUpdatedEvent;
import com.openlattice.edm.exceptions.TypeExistsException;
import com.openlattice.edm.exceptions.TypeNotFoundException;
import com.openlattice.edm.types.processors.AddDstEntityTypesToAssociationTypeProcessor;
import com.openlattice.edm.types.processors.AddPrimaryKeysToEntityTypeProcessor;
import com.openlattice.edm.types.processors.AddPropertyTypesToEntityTypeProcessor;
import com.openlattice.edm.types.processors.AddSrcEntityTypesToAssociationTypeProcessor;
import com.openlattice.edm.types.processors.RemoveDstEntityTypesFromAssociationTypeProcessor;
import com.openlattice.edm.types.processors.RemovePrimaryKeysFromEntityTypeProcessor;
import com.openlattice.edm.types.processors.RemovePropertyTypesFromEntityTypeProcessor;
import com.openlattice.edm.types.processors.RemoveSrcEntityTypesFromAssociationTypeProcessor;
import com.openlattice.edm.types.processors.ReorderPropertyTypesInEntityTypeProcessor;
import com.openlattice.edm.types.processors.UpdateEntitySetMetadataProcessor;
import com.openlattice.edm.types.processors.UpdateEntitySetPropertyMetadataProcessor;
import com.openlattice.edm.types.processors.UpdateEntityTypeMetadataProcessor;
import com.openlattice.edm.types.processors.UpdatePropertyTypeMetadataProcessor;
import com.openlattice.hazelcast.HazelcastUtils;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.EntryProcessor;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.HazelcastAclKeyReservationService;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.Principals;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.data.DatasourceManager;
import com.openlattice.datastore.util.Util;
import com.openlattice.edm.EntityDataModel;
import com.openlattice.edm.EntityDataModelDiff;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.Schema;
import com.openlattice.edm.properties.PostgresTypeManager;
import com.openlattice.edm.requests.MetadataUpdate;
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager;
import com.openlattice.edm.set.EntitySetPropertyKey;
import com.openlattice.edm.set.EntitySetPropertyMetadata;
import com.openlattice.edm.type.AssociationDetails;
import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.ComplexType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.EnumType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.postgres.PostgresQuery;
import com.openlattice.postgres.PostgresTablesPod;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EdmService implements EdmManager {

    private static final Logger logger = LoggerFactory.getLogger( EdmService.class );
    private final IMap<String, UUID> edmVersions;

    private final IMap<UUID, PropertyType>                              propertyTypes;
    private final IMap<UUID, ComplexType>                               complexTypes;
    private final IMap<UUID, EnumType>                                  enumTypes;
    private final IMap<UUID, EntityType>                                entityTypes;
    private final IMap<UUID, EntitySet>                                 entitySets;
    private final IMap<String, UUID>                                    aclKeys;
    private final IMap<UUID, String>                                    names;
    private final IMap<UUID, AssociationType>                           associationTypes;
    private final IMap<UUID, UUID>                                      syncIds;
    private final IMap<EntitySetPropertyKey, EntitySetPropertyMetadata> entitySetPropertyMetadata;
    private final IMap<AclKey, SecurableObjectType>                     securableObjectTypes;

    private final HazelcastAclKeyReservationService aclKeyReservations;
    private final AuthorizationManager              authorizations;
    private final PostgresEntitySetManager          entitySetManager;
    private final PostgresTypeManager               entityTypeManager;
    private final HazelcastSchemaManager            schemaManager;
    private final DatasourceManager                 datasourceManager;

    private final HazelcastInstance hazelcastInstance;
    private final HikariDataSource  hds;

    @Inject
    private EventBus eventBus;

    public EdmService(
            HikariDataSource hds,
            HazelcastInstance hazelcastInstance,
            HazelcastAclKeyReservationService aclKeyReservations,
            AuthorizationManager authorizations,
            PostgresEntitySetManager entitySetManager,
            PostgresTypeManager entityTypeManager,
            HazelcastSchemaManager schemaManager,
            DatasourceManager datasourceManager ) {

        this.authorizations = authorizations;
        this.entitySetManager = entitySetManager;
        this.entityTypeManager = entityTypeManager;
        this.schemaManager = schemaManager;
        this.hazelcastInstance = hazelcastInstance;
        this.hds = hds;
        this.edmVersions = hazelcastInstance.getMap( HazelcastMap.EDM_VERSIONS.name() );
        this.propertyTypes = hazelcastInstance.getMap( HazelcastMap.PROPERTY_TYPES.name() );
        this.complexTypes = hazelcastInstance.getMap( HazelcastMap.COMPLEX_TYPES.name() );
        this.enumTypes = hazelcastInstance.getMap( HazelcastMap.ENUM_TYPES.name() );
        this.entityTypes = hazelcastInstance.getMap( HazelcastMap.ENTITY_TYPES.name() );
        this.entitySets = hazelcastInstance.getMap( HazelcastMap.ENTITY_SETS.name() );
        this.names = hazelcastInstance.getMap( HazelcastMap.NAMES.name() );
        this.aclKeys = hazelcastInstance.getMap( HazelcastMap.ACL_KEYS.name() );
        this.associationTypes = hazelcastInstance.getMap( HazelcastMap.ASSOCIATION_TYPES.name() );
        this.syncIds = hazelcastInstance.getMap( HazelcastMap.SYNC_IDS.name() );
        this.entitySetPropertyMetadata = hazelcastInstance.getMap( HazelcastMap.ENTITY_SET_PROPERTY_METADATA.name() );
        this.securableObjectTypes = hazelcastInstance.getMap( HazelcastMap.SECURABLE_OBJECT_TYPES.name() );
        this.aclKeyReservations = aclKeyReservations;
        this.datasourceManager = datasourceManager;
        propertyTypes.values().forEach( propertyType -> logger.debug( "Property type read: {}", propertyType ) );
        entityTypes.values().forEach( entityType -> logger.debug( "Object type read: {}", entityType ) );
    }

    @Override
    public void clearTables() {
        eventBus.post( new ClearAllDataEvent() );
        for ( int i = 0; i < HazelcastMap.values().length; i++ ) {
            hazelcastInstance.getMap( HazelcastMap.values()[ i ].name() ).clear();
        }
        try ( java.sql.Connection connection = hds.getConnection() ) {
            new PostgresTablesPod().postgresTables().tables().forEach( table -> {
                try ( PreparedStatement ps = connection
                        .prepareStatement( PostgresQuery.truncate( table.getName() ) ) ) {
                    ps.execute();
                } catch ( SQLException e ) {
                    logger.debug( "Unable to truncate table {}", table.getName(), e );
                }
            } );
            connection.close();

        } catch ( SQLException e ) {
            logger.debug( "Unable to clear all data.", e );
        }
    }

    @Override
    public UUID getCurrentEntityDataModelVersion() {
        if ( !edmVersions.containsKey( EntityDataModel.getEdmVersionKey() ) ) {
            return generateNewEntityDataModelVersion();
        }
        return edmVersions.get( EntityDataModel.getEdmVersionKey() );
    }

    @Override
    public UUID generateNewEntityDataModelVersion() {
        UUID newVersion = new UUID( System.currentTimeMillis(), 0 );
        edmVersions.put( EntityDataModel.getEdmVersionKey(), newVersion );
        return newVersion;
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.types.services.EdmManager#createPropertyType(com.kryptnostic.types.PropertyType) update
     * propertyType (and return true upon success) if exists, return false otherwise
     */
    @Override
    public void createPropertyTypeIfNotExists( PropertyType propertyType ) {
        try {
            aclKeyReservations.reserveIdAndValidateType( propertyType );
        } catch ( TypeExistsException e ) {
            logger.error( "A type with this name already exists." );
            return;
        }

        /*
         * Create property type if it doesn't exists. The reserveAclKeyAndValidateType call should ensure that
         */

        PropertyType dbRecord = propertyTypes.putIfAbsent( propertyType.getId(), propertyType );

        if ( dbRecord == null ) {
            propertyType.getSchemas().forEach( schemaManager.propertyTypesSchemaAdder( propertyType.getId() ) );
            eventBus.post( new PropertyTypeCreatedEvent( propertyType ) );
        } else {
            logger.error(
                    "Inconsistency encountered in database. Verify that existing property types have all their acl keys reserved." );
        }
    }

    @Override
    public void deleteEntityType( UUID entityTypeId ) {
        /*
         * Entity types should only be deleted if there are no entity sets of that type in the system.
         */
        if ( Iterables.isEmpty( entitySetManager.getAllEntitySetsForType( entityTypeId ) ) ) {
            entityTypeManager.getAssociationIdsForEntityType( entityTypeId ).forEach( associationTypeId -> {
                AssociationType association = getAssociationType( associationTypeId );
                if ( association.getSrc().contains( entityTypeId ) ) {
                    removeSrcEntityTypesFromAssociationType( associationTypeId, ImmutableSet.of( entityTypeId ) );
                }
                if ( association.getDst().contains( entityTypeId ) ) {
                    removeDstEntityTypesFromAssociationType( associationTypeId, ImmutableSet.of( entityTypeId ) );
                }
            } );

            entityTypes.delete( entityTypeId );
            aclKeyReservations.release( entityTypeId );
            eventBus.post( new EntityTypeDeletedEvent( entityTypeId ) );
        } else {
            throw new IllegalArgumentException(
                    "Unable to delete entity type because it is associated with an entity set." );
        }
    }

    @Override
    public void deletePropertyType( UUID propertyTypeId ) {
        Stream<EntityType> entityTypes = entityTypeManager
                .getEntityTypesContainingPropertyTypesAsStream( ImmutableSet.of( propertyTypeId ) );
        if ( entityTypes
                .allMatch( et -> Iterables.isEmpty( entitySetManager.getAllEntitySetsForType( et.getId() ) ) ) ) {
            forceDeletePropertyType( propertyTypeId );
        } else {
            throw new IllegalArgumentException(
                    "Unable to delete property type because it is associated with an entity set." );
        }
    }

    @Override
    public void forceDeletePropertyType( UUID propertyTypeId ) {
        Stream<EntityType> entityTypes = entityTypeManager
                .getEntityTypesContainingPropertyTypesAsStream( ImmutableSet.of( propertyTypeId ) );
        entityTypes.forEach( et -> {
            forceRemovePropertyTypesFromEntityType( et.getId(), ImmutableSet.of( propertyTypeId ) );
        } );

        propertyTypes.delete( propertyTypeId );
        aclKeyReservations.release( propertyTypeId );
        eventBus.post( new PropertyTypeDeletedEvent( propertyTypeId ) );
    }

    private EntityType getEntityTypeWithBaseType( EntityType entityType ) {
        EntityType baseType = getEntityType( entityType.getBaseType().get() );
        LinkedHashSet<UUID> properties = new LinkedHashSet<UUID>();
        properties.addAll( baseType.getProperties() );
        properties.addAll( entityType.getProperties() );
        LinkedHashSet<UUID> key = new LinkedHashSet<UUID>();
        key.addAll( baseType.getKey() );
        key.addAll( entityType.getKey() );
        key.forEach( keyId -> Preconditions.checkArgument( properties.contains( keyId ),
                "Properties must include all the key property types" ) );
        return new EntityType(
                Optional.fromNullable( entityType.getId() ),
                entityType.getType(),
                entityType.getTitle(),
                Optional.fromNullable( entityType.getDescription() ),
                entityType.getSchemas(),
                key,
                properties,
                entityType.getBaseType(),
                Optional.fromNullable( entityType.getCategory() ) );

    }

    public void createEntityType( EntityType entityTypeRaw ) {
        /*
         * This is really create or replace and should be noted as such.
         */
        EntityType entityType = ( entityTypeRaw.getBaseType().isPresent() )
                ? getEntityTypeWithBaseType( entityTypeRaw ) : entityTypeRaw;
        aclKeyReservations.reserveIdAndValidateType( entityType );
        // Only create entity table if insert transaction succeeded.
        final EntityType existing = entityTypes.putIfAbsent( entityType.getId(), entityType );
        if ( existing == null ) {
            /*
             * As long as schemas are registered with upsertSchema, the schema query service should pick up the schemas
             * directly from the entity types and property types tables. Longer term, we should be more explicit about
             * the magic schema registration that happens when an entity type or property type is written since the
             * services are loosely coupled in a way that makes it easy to break accidentally.
             */
            schemaManager.upsertSchemas( entityType.getSchemas() );
            if ( !entityType.getCategory().equals( SecurableObjectType.AssociationType ) ) {
                eventBus.post( new EntityTypeCreatedEvent( entityType ) );
            }
        } else {
            /*
             * Only allow updates if entity type is not already in use.
             */
            if ( Iterables.isEmpty(
                    entitySetManager.getAllEntitySetsForType( entityType.getId() ) ) ) {
                // Retrieve properties known to user
                Set<UUID> currentPropertyTypes = existing.getProperties();
                // Remove the removable property types in database properly; this step takes care of removal of
                // permissions
                Set<UUID> removablePropertyTypesInEntityType = Sets.difference( currentPropertyTypes,
                        entityType.getProperties() );
                removePropertyTypesFromEntityType( existing.getId(), removablePropertyTypesInEntityType );
                // Add the new property types in
                Set<UUID> newPropertyTypesInEntityType = Sets.difference( entityType.getProperties(),
                        currentPropertyTypes );
                addPropertyTypesToEntityType( entityType.getId(), newPropertyTypesInEntityType );

                // Update Schema
                final Set<FullQualifiedName> currentSchemas = existing.getSchemas();
                final Set<FullQualifiedName> removableSchemas = Sets.difference( currentSchemas,
                        entityType.getSchemas() );
                final Set<UUID> entityTypeSingleton = getEntityTypeUuids( ImmutableSet.of( existing.getType() ) );

                removableSchemas
                        .forEach( schema -> schemaManager.removeEntityTypesFromSchema( entityTypeSingleton, schema ) );

                Set<FullQualifiedName> newSchemas = Sets.difference( entityType.getSchemas(), currentSchemas );
                newSchemas.forEach( schema -> schemaManager.addEntityTypesToSchema( entityTypeSingleton, schema ) );
            }
        }
    }

    /**
     * Remove permissions/metadata information of the entity set
     */
    @Override
    public void deleteEntitySet( UUID entitySetId ) {
        final EntitySet entitySet = Util.getSafely( entitySets, entitySetId );
        final EntityType entityType = getEntityType( entitySet.getEntityTypeId() );

        /*
         * We cleanup permissions first as this will make entity set unavailable, even if delete fails.
         */
        authorizations.deletePermissions( new AclKey( entitySetId ) );
        entityType.getProperties().stream()
                .map( propertyTypeId -> new AclKey( entitySetId, propertyTypeId ) )
                .forEach( aclKey -> {
                    authorizations.deletePermissions( aclKey );
                    entitySetPropertyMetadata.delete( new EntitySetPropertyKey( aclKey.get( 0 ), aclKey.get( 1 ) ) );
                } );

        Util.deleteSafely( entitySets, entitySetId );
        aclKeyReservations.release( entitySetId );
        syncIds.remove( entitySetId );
        eventBus.post( new EntitySetDeletedEvent( entitySetId ) );
    }

    private void createEntitySet( EntitySet entitySet ) {
        aclKeyReservations.reserveIdAndValidateType( entitySet );

        checkState( entitySets.putIfAbsent( entitySet.getId(), entitySet ) == null, "Entity set already exists." );
        datasourceManager.setCurrentSyncId( entitySet.getId(),
                datasourceManager.createNewSyncIdForEntitySet( entitySet.getId() ) );
    }

    @Override
    public void createEntitySet( Principal principal, EntitySet entitySet ) {
        EntityType entityType = entityTypes.get( entitySet.getEntityTypeId() );
        createEntitySet( principal, entitySet, entityType.getProperties() );
    }

    @Override
    public void createEntitySet( Principal principal, EntitySet entitySet, Set<UUID> ownablePropertyTypes ) {
        Principals.ensureUser( principal );
        createEntitySet( entitySet );

        try {
            setupDefaultEntitySetPropertyMetadata( entitySet.getId(), entitySet.getEntityTypeId() );

            authorizations.setSecurableObjectType( new AclKey( entitySet.getId() ), SecurableObjectType.EntitySet );

            authorizations.addPermission( new AclKey( entitySet.getId() ),
                    principal,
                    EnumSet.allOf( Permission.class ) );

            ownablePropertyTypes.stream()
                    .map( propertyTypeId -> new AclKey( entitySet.getId(), propertyTypeId ) )
                    .peek( aclKey -> {
                        authorizations.setSecurableObjectType( aclKey,
                                SecurableObjectType.PropertyTypeInEntitySet );
                    } ).forEach( aclKey -> authorizations.addPermission(
                    aclKey,
                    principal,
                    EnumSet.allOf( Permission.class ) ) );

            eventBus.post( new EntitySetCreatedEvent( entitySet,
                    Lists.newArrayList( propertyTypes.getAll( ownablePropertyTypes ).values() ) ) );

        } catch ( Exception e ) {
            logger.error( "Unable to create entity set {} for principal {}", entitySet, principal, e );
            Util.deleteSafely( entitySets, entitySet.getId() );
            aclKeyReservations.release( entitySet.getId() );
            throw new IllegalStateException( "Unable to create entity set: " + entitySet.getId() );
        }
    }

    private void setupDefaultEntitySetPropertyMetadata( UUID entitySetId, UUID entityTypeId ) {
        getEntityType( entityTypeId ).getProperties().forEach( propertyTypeId -> {
            EntitySetPropertyKey key = new EntitySetPropertyKey( entitySetId, propertyTypeId );
            PropertyType property = getPropertyType( propertyTypeId );
            EntitySetPropertyMetadata metadata = new EntitySetPropertyMetadata(
                    property.getTitle(),
                    property.getDescription(),
                    true );
            entitySetPropertyMetadata.put( key, metadata );
        } );
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public Set<UUID> getAclKeys( Set<?> fqnsOrNames ) {
        if ( fqnsOrNames.isEmpty() ) {
            return ImmutableSet.of();
        }

        Object o = fqnsOrNames.iterator().next();
        Set<String> names;
        if ( String.class.isAssignableFrom( o.getClass() ) ) {
            names = (Set<String>) fqnsOrNames;
        } else if ( FullQualifiedName.class.isAssignableFrom( o.getClass() ) ) {
            names = Util.fqnToString( (Set<FullQualifiedName>) fqnsOrNames );
        } else {
            throw new IllegalArgumentException(
                    "Unable to retrieve Acl Keys for this type: " + o.getClass().getSimpleName() );
        }
        return ImmutableSet.copyOf( aclKeys.getAll( names ).values() );
    }

    @Override
    public Set<UUID> getEntityTypeUuids( Set<FullQualifiedName> fqns ) {
        return aclKeys.getAll( Util.fqnToString( fqns ) ).values().stream()
                .filter( id -> id != null )
                .collect( Collectors.toSet() );
    }

    @Override
    public Set<UUID> getPropertyTypeUuids( Set<FullQualifiedName> fqns ) {
        return aclKeys.getAll( Util.fqnToString( fqns ) ).values().stream()
                .filter( id -> id != null )
                .collect( Collectors.toSet() );
    }

    @Override
    public void createEnumTypeIfNotExists( EnumType enumType ) {
        aclKeyReservations.reserveIdAndValidateType( enumType );
        enumTypes.putIfAbsent( enumType.getId(), enumType );
    }

    @Override
    public Stream<EnumType> getEnumTypes() {
        return entityTypeManager.getEnumTypeIds()
                .parallel()
                .map( enumTypes::get );
    }

    @Override
    public EnumType getEnumType( UUID enumTypeId ) {
        return enumTypes.get( enumTypeId );
    }

    @Override
    public Set<EntityType> getEntityTypeHierarchy( UUID entityTypeId ) {
        return getTypeHierarchy( entityTypeId, HazelcastUtils.getter( entityTypes ), EntityType::getBaseType );
    }

    @Override
    public void deleteEnumType( UUID enumTypeId ) {
        enumTypes.delete( enumTypeId );
    }

    @Override
    public void createComplexTypeIfNotExists( ComplexType complexType ) {
        aclKeyReservations.reserveIdAndValidateType( complexType );
        complexTypes.putIfAbsent( complexType.getId(), complexType );
    }

    @Override
    public Stream<ComplexType> getComplexTypes() {
        /*
         * An assumption worth stating here is that we are going to periodically run health checks the verify the
         * consistency of the database such that no null values will ever be present.
         */
        return entityTypeManager.getComplexTypeIds()
                .parallel()
                .map( complexTypes::get );
    }

    @Override
    public ComplexType getComplexType( UUID complexTypeId ) {
        return complexTypes.get( complexTypeId );
    }

    @Override
    public Set<ComplexType> getComplexTypeHierarchy( UUID complexTypeId ) {
        return getTypeHierarchy( complexTypeId, HazelcastUtils.getter( complexTypes ), ComplexType::getBaseType );
    }

    private <T> Set<T> getTypeHierarchy(
            UUID enumTypeId,
            Function<UUID, T> typeGetter,
            Function<T, Optional<UUID>> baseTypeSupplier ) {
        Set<T> typeHierarchy = new LinkedHashSet<>();
        T entityType;
        Optional<UUID> baseType = Optional.of( enumTypeId );

        do {
            entityType = typeGetter.apply( baseType.get() );
            if ( entityType == null ) {
                throw new TypeNotFoundException( "Unable to find type " + baseType.get() );
            }
            baseType = baseTypeSupplier.apply( entityType );
            typeHierarchy.add( entityType );
        } while ( baseType.isPresent() );

        return typeHierarchy;
    }

    @Override
    public void deleteComplexType( UUID complexTypeId ) {
        complexTypes.delete( complexTypeId );
    }

    @Override
    public EntityType getEntityType( FullQualifiedName typeFqn ) {
        UUID entityTypeId = getTypeAclKey( typeFqn );
        Preconditions.checkNotNull( entityTypeId,
                "Entity type %s does not exists.",
                typeFqn.getFullQualifiedNameAsString() );
        return getEntityType( entityTypeId );
    }

    @Override
    public EntityType getEntityType( UUID entityTypeId ) {
        return Preconditions.checkNotNull(
                getEntityTypeSafe( entityTypeId ),
                "Entity type of id %s does not exists.",
                entityTypeId.toString() );
    }

    @Override
    public EntityType getEntityTypeSafe( UUID entityTypeId ) {
        return Util.getSafely( entityTypes, entityTypeId );
    }

    public Iterable<EntityType> getEntityTypes() {
        return entityTypeManager.getEntityTypes();
    }

    public Iterable<EntityType> getEntityTypesStrict() {
        return entityTypeManager.getEntityTypesStrict();
    }

    @Override
    public Iterable<EntityType> getAssociationEntityTypes() {
        return entityTypeManager.getAssociationEntityTypes();
    }

    @Override
    public Iterable<AssociationType> getAssociationTypes() {
        return Iterables.transform( entityTypeManager.getAssociationTypeIds(),
                associationTypeId -> getAssociationType( associationTypeId ) );
    }

    @Override
    public EntityType getEntityType( String namespace, String name ) {
        return getEntityType( new FullQualifiedName( namespace, name ) );
    }

    @Override
    public EntitySet getEntitySet( UUID entitySetId ) {
        return Util.getSafely( entitySets, entitySetId );
    }

    @Override
    public Iterable<EntitySet> getEntitySets() {
        return entitySetManager.getAllEntitySets();
        // return StreamSupport
        // .stream( entitySetIds.spliterator(), false )
        // .map( Util.getSafeMapper( entitySets ) )::iterator;
    }

    @Override
    public PropertyType getPropertyType( FullQualifiedName propertyType ) {
        return Preconditions.checkNotNull(
                Util.getSafely( propertyTypes, Util.getSafely( aclKeys, Util.fqnToString( propertyType ) ) ),
                "Property type %s does not exists",
                propertyType.getFullQualifiedNameAsString() );
    }

    @Override
    public Iterable<PropertyType> getPropertyTypesInNamespace( String namespace ) {
        return entityTypeManager.getPropertyTypesInNamespace( namespace );
    }

    @Override
    public Iterable<PropertyType> getPropertyTypes() {
        return entityTypeManager.getPropertyTypes();
    }

    @Override
    public void addPropertyTypesToEntityType( UUID entityTypeId, Set<UUID> propertyTypeIds ) {
        Preconditions.checkArgument( checkPropertyTypesExist( propertyTypeIds ), "Some properties do not exists." );
        Stream<UUID> childrenIds = entityTypeManager.getEntityTypeChildrenIdsDeep( entityTypeId );
        Map<UUID, Boolean> childrenIdsToLocks = childrenIds
                .collect( Collectors.toMap( Functions.<UUID>identity()::apply, propertyTypes::tryLock ) );
        childrenIdsToLocks.values().forEach( locked -> {
            if ( !locked ) {
                childrenIdsToLocks.entrySet().forEach( entry -> {
                    if ( entry.getValue() ) { propertyTypes.unlock( entry.getKey() ); }
                } );
                throw new IllegalStateException(
                        "Unable to modify the entity data model right now--please try again." );
            }
        } );
        childrenIdsToLocks.keySet().forEach( id -> {
            entityTypes.executeOnKey( id, new AddPropertyTypesToEntityTypeProcessor( propertyTypeIds ) );

            for ( EntitySet entitySet : entitySetManager.getAllEntitySetsForType( id ) ) {
                UUID esId = entitySet.getId();
                Map<UUID, PropertyType> propertyTypes = propertyTypeIds.stream().collect( Collectors.toMap(
                        propertyTypeId -> propertyTypeId, propertyTypeId -> getPropertyType( propertyTypeId ) ) );
                Iterable<Principal> owners = authorizations.getSecurableObjectOwners( new AclKey( esId ) );
                for ( Principal owner : owners ) {
                    propertyTypeIds.stream()
                            .map( propertyTypeId -> new AclKey( entitySet.getId(), propertyTypeId ) )
                            .forEach( aclKey -> {
                                authorizations.setSecurableObjectType( aclKey,
                                        SecurableObjectType.PropertyTypeInEntitySet );

                                authorizations.addPermission(
                                        aclKey,
                                        owner,
                                        EnumSet.allOf( Permission.class ) );

                                PropertyType pt = propertyTypes.get( aclKey.get( 1 ) );
                                EntitySetPropertyMetadata defaultMetadata = new EntitySetPropertyMetadata(
                                        pt.getTitle(),
                                        pt.getDescription(),
                                        true );
                                entitySetPropertyMetadata.put(
                                        new EntitySetPropertyKey( aclKey.get( 0 ), aclKey.get( 1 ) ), defaultMetadata );
                            } );
                }
            }
            EntityType entityType = getEntityType( id );
            if ( !entityType.getCategory().equals( SecurableObjectType.AssociationType ) ) {
                eventBus.post( new EntityTypeCreatedEvent( entityType ) );
            } else {
                eventBus.post( new AssociationTypeCreatedEvent( getAssociationType( id ) ) );
            }
        } );
        childrenIdsToLocks.entrySet().forEach( entry -> {
            if ( entry.getValue() ) { propertyTypes.unlock( entry.getKey() ); }
        } );
    }

    @Override
    public void removePropertyTypesFromEntityType( UUID entityTypeId, Set<UUID> propertyTypeIds ) {
        Preconditions.checkArgument( checkPropertyTypesExist( propertyTypeIds ), "Some properties do not exists." );

        List<UUID> childrenIds = entityTypeManager.getEntityTypeChildrenIdsDeep( entityTypeId )
                .collect( Collectors.<UUID>toList() );
        childrenIds.forEach( id -> {
            Preconditions.checkArgument( Sets.intersection( getEntityType( id ).getKey(), propertyTypeIds ).isEmpty(),
                    "Key property types cannot be removed." );
            Preconditions.checkArgument( !entitySetManager.getAllEntitySetsForType( id ).iterator().hasNext(),
                    "Property types cannot be removed from entity types that have already been associated with an entity set." );
        } );

        forceRemovePropertyTypesFromEntityType( entityTypeId, propertyTypeIds );

    }

    @Override
    public void forceRemovePropertyTypesFromEntityType( UUID entityTypeId, Set<UUID> propertyTypeIds ) {
        Preconditions.checkArgument( checkPropertyTypesExist( propertyTypeIds ), "Some properties do not exists." );
        EntityType entityType = getEntityType( entityTypeId );
        if ( entityType.getBaseType().isPresent() ) {
            EntityType baseType = getEntityType( entityType.getBaseType().get() );
            Preconditions.checkArgument( Sets.intersection( propertyTypeIds, baseType.getProperties() ).isEmpty(),
                    "Inherited property types cannot be removed." );
        }

        List<UUID> childrenIds = entityTypeManager.getEntityTypeChildrenIdsDeep( entityTypeId )
                .collect( Collectors.<UUID>toList() );

        Map<UUID, Boolean> childrenIdsToLocks = childrenIds.stream()
                .collect( Collectors.toMap( Functions.<UUID>identity()::apply, propertyTypes::tryLock ) );
        childrenIdsToLocks.values().forEach( locked -> {
            if ( !locked ) {
                childrenIdsToLocks.entrySet().forEach( entry -> {
                    if ( entry.getValue() ) { propertyTypes.unlock( entry.getKey() ); }
                } );
                throw new IllegalStateException(
                        "Unable to modify the entity data model right now--please try again." );
            }
        } );
        childrenIds.forEach( id -> {
            entityTypes.executeOnKey( id, new RemovePropertyTypesFromEntityTypeProcessor( propertyTypeIds ) );
            EntityType childEntityType = getEntityType( id );
            if ( !childEntityType.getCategory().equals( SecurableObjectType.AssociationType ) ) {
                eventBus.post( new EntityTypeCreatedEvent( childEntityType ) );
            } else {
                eventBus.post( new AssociationTypeCreatedEvent( getAssociationType( id ) ) );
            }
        } );
        childrenIds.forEach( propertyTypes::unlock );

    }

    @Override
    public void reorderPropertyTypesInEntityType( UUID entityTypeId, LinkedHashSet<UUID> propertyTypeIds ) {
        entityTypes.executeOnKey( entityTypeId, new ReorderPropertyTypesInEntityTypeProcessor( propertyTypeIds ) );
        EntityType entityType = getEntityType( entityTypeId );
        if ( entityType.getCategory().equals( SecurableObjectType.AssociationType ) ) {
            eventBus.post( new AssociationTypeCreatedEvent( getAssociationType( entityTypeId ) ) );
        } else {
            eventBus.post( new EntityTypeCreatedEvent( entityType ) );
        }
    }

    @Override
    public void addPrimaryKeysToEntityType( UUID entityTypeId, Set<UUID> propertyTypeIds ) {
        Preconditions.checkArgument( checkPropertyTypesExist( propertyTypeIds ), "Some properties do not exists." );
        EntityType entityType = entityTypes.get( entityTypeId );
        Preconditions.checkNotNull( entityType, "No entity type with id {}", entityTypeId );
        Preconditions.checkArgument( entityType.getProperties().containsAll( propertyTypeIds ),
                "Entity type does not contain all the requested primary key property types." );

        entityTypes.executeOnKey( entityTypeId, new AddPrimaryKeysToEntityTypeProcessor( propertyTypeIds ) );

        entityType = entityTypes.get( entityTypeId );
        if ( entityType.getCategory().equals( SecurableObjectType.AssociationType ) ) {
            eventBus.post( new AssociationTypeCreatedEvent( getAssociationType( entityTypeId ) ) );
        } else {
            eventBus.post( new EntityTypeCreatedEvent( entityType ) );
        }
    }

    @Override
    public void removePrimaryKeysFromEntityType( UUID entityTypeId, Set<UUID> propertyTypeIds ) {
        Preconditions.checkArgument( checkPropertyTypesExist( propertyTypeIds ), "Some properties do not exists." );
        EntityType entityType = entityTypes.get( entityTypeId );
        Preconditions.checkNotNull( entityType, "No entity type with id {}", entityTypeId );
        Preconditions.checkArgument( entityType.getProperties().containsAll( propertyTypeIds ),
                "Entity type does not contain all the requested primary key property types." );

        entityTypes.executeOnKey( entityTypeId, new RemovePrimaryKeysFromEntityTypeProcessor( propertyTypeIds ) );

        entityType = entityTypes.get( entityTypeId );
        if ( entityType.getCategory().equals( SecurableObjectType.AssociationType ) ) {
            eventBus.post( new AssociationTypeCreatedEvent( getAssociationType( entityTypeId ) ) );
        } else {
            eventBus.post( new EntityTypeCreatedEvent( entityType ) );
        }
    }

    @Override
    public void addSrcEntityTypesToAssociationType( UUID associationTypeId, Set<UUID> entityTypeIds ) {
        Preconditions.checkArgument( checkEntityTypesExist( entityTypeIds ) );
        associationTypes.executeOnKey( associationTypeId,
                new AddSrcEntityTypesToAssociationTypeProcessor( entityTypeIds ) );
        eventBus.post( new AssociationTypeCreatedEvent( getAssociationType( associationTypeId ) ) );
    }

    @Override
    public void addDstEntityTypesToAssociationType( UUID associationTypeId, Set<UUID> entityTypeIds ) {
        Preconditions.checkArgument( checkEntityTypesExist( entityTypeIds ) );
        associationTypes.executeOnKey( associationTypeId,
                new AddDstEntityTypesToAssociationTypeProcessor( entityTypeIds ) );
        eventBus.post( new AssociationTypeCreatedEvent( getAssociationType( associationTypeId ) ) );
    }

    @Override
    public void removeSrcEntityTypesFromAssociationType( UUID associationTypeId, Set<UUID> entityTypeIds ) {
        Preconditions.checkArgument( checkEntityTypesExist( entityTypeIds ) );
        associationTypes.executeOnKey( associationTypeId,
                new RemoveSrcEntityTypesFromAssociationTypeProcessor( entityTypeIds ) );
        eventBus.post( new AssociationTypeCreatedEvent( getAssociationType( associationTypeId ) ) );
    }

    @Override
    public void removeDstEntityTypesFromAssociationType( UUID associationTypeId, Set<UUID> entityTypeIds ) {
        Preconditions.checkArgument( checkEntityTypesExist( entityTypeIds ) );
        associationTypes.executeOnKey( associationTypeId,
                new RemoveDstEntityTypesFromAssociationTypeProcessor( entityTypeIds ) );
        eventBus.post( new AssociationTypeCreatedEvent( getAssociationType( associationTypeId ) ) );
    }

    @Override
    public void updatePropertyTypeMetadata( UUID propertyTypeId, MetadataUpdate update ) {
        if ( update.getType().isPresent() ) {
            aclKeyReservations.renameReservation( propertyTypeId, update.getType().get() );
        }
        propertyTypes.executeOnKey( propertyTypeId, new UpdatePropertyTypeMetadataProcessor( update ) );
        // get all entity sets containing the property type, and re-index them.
        entityTypeManager
                .getEntityTypesContainingPropertyTypesAsStream( ImmutableSet.of( propertyTypeId ) )
                .forEach( et -> {
                    List<PropertyType> properties = Lists
                            .newArrayList( propertyTypes.getAll( et.getProperties() ).values() );
                    entitySetManager.getAllEntitySetsForType( et.getId() )
                            .forEach( es -> eventBus
                                    .post( new PropertyTypesInEntitySetUpdatedEvent( es.getId(), properties ) ) );
                } );
        eventBus.post( new PropertyTypeCreatedEvent( getPropertyType( propertyTypeId ) ) );
    }

    @Override
    public void updateEntityTypeMetadata( UUID entityTypeId, MetadataUpdate update ) {
        if ( update.getType().isPresent() ) {
            aclKeyReservations.renameReservation( entityTypeId, update.getType().get() );
        }
        entityTypes.executeOnKey( entityTypeId, new UpdateEntityTypeMetadataProcessor( update ) );
        if ( !getEntityType( entityTypeId ).getCategory().equals( SecurableObjectType.AssociationType ) ) {
            eventBus.post( new EntityTypeCreatedEvent( getEntityType( entityTypeId ) ) );
        } else {
            eventBus.post( new AssociationTypeCreatedEvent( getAssociationType( entityTypeId ) ) );
        }
    }

    @Override
    public void updateEntitySetMetadata( UUID entitySetId, MetadataUpdate update ) {
        if ( update.getName().isPresent() ) {
            aclKeyReservations.renameReservation( entitySetId, update.getName().get() );
        }
        entitySets.executeOnKey( entitySetId, new UpdateEntitySetMetadataProcessor( update ) );
        eventBus.post( new EntitySetMetadataUpdatedEvent( getEntitySet( entitySetId ) ) );
    }

    /**************
     * Validation
     **************/
    @Override
    public boolean checkPropertyTypesExist( Set<UUID> properties ) {
        return properties.stream().allMatch( propertyTypes::containsKey );
    }

    @Override
    public boolean checkPropertyTypeExists( UUID propertyTypeId ) {
        return propertyTypes.containsKey( propertyTypeId );
    }

    @Override
    public boolean checkEntityTypesExist( Set<UUID> entityTypeIds ) {
        return entityTypeIds.stream().allMatch( entityTypes::containsKey );
    }

    @Override
    public boolean checkEntityTypeExists( UUID entityTypeId ) {
        return entityTypes.containsKey( entityTypeId );
    }

    @Override
    public boolean checkEntitySetExists( String name ) {
        UUID id = Util.getSafely( aclKeys, name );
        if ( id == null ) {
            return false;
        } else {
            return entitySets.containsKey( id );
        }
    }

    @Override
    public Collection<PropertyType> getPropertyTypes( Set<UUID> propertyIds ) {
        return propertyTypes.getAll( propertyIds ).values();
    }

    @Override
    public UUID getTypeAclKey( FullQualifiedName type ) {
        return Util.getSafely( aclKeys, Util.fqnToString( type ) );
    }

    @Override
    public PropertyType getPropertyType( UUID propertyTypeId ) {
        return Util.getSafely( propertyTypes, propertyTypeId );
    }

    @Override
    public FullQualifiedName getPropertyTypeFqn( UUID propertyTypeId ) {
        return Util.stringToFqn( Util.getSafely( names, propertyTypeId ) );
    }

    @Override
    public FullQualifiedName getEntityTypeFqn( UUID entityTypeId ) {
        return Util.stringToFqn( Util.getSafely( names, entityTypeId ) );
    }

    @Override
    public EntitySet getEntitySet( String entitySetName ) {
        UUID id = Util.getSafely( aclKeys, entitySetName );
        if ( id == null ) {
            return null;
        } else {
            return getEntitySet( id );
        }
    }

    @Override
    public Map<UUID, PropertyType> getPropertyTypesAsMap( Set<UUID> propertyTypeIds ) {
        return propertyTypes.getAll( propertyTypeIds );
    }

    @Override
    public Map<UUID, EntityType> getEntityTypesAsMap( Set<UUID> entityTypeIds ) {
        return entityTypes.getAll( entityTypeIds );
    }

    @Override
    public Map<UUID, EntitySet> getEntitySetsAsMap( Set<UUID> entitySetIds ) {
        return entitySets.getAll( entitySetIds );
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public <V> Map<UUID, V> fromPropertyTypes( Set<UUID> propertyTypeIds, EntryProcessor<UUID, PropertyType> ep ) {
        return (Map<UUID, V>) propertyTypes.executeOnKeys( propertyTypeIds, ep );
    }

    @Override
    public Set<UUID> getPropertyTypeUuidsOfEntityTypeWithPIIField( UUID entityTypeId ) {
        return getEntityType( entityTypeId ).getProperties().stream()
                .filter( ptId -> getPropertyType( ptId ).isPIIfield() ).collect( Collectors.toSet() );
    }

    @Override
    public EntityType getEntityTypeByEntitySetId( UUID entitySetId ) {
        UUID entityTypeId = getEntitySet( entitySetId ).getEntityTypeId();
        return getEntityType( entityTypeId );
    }

    @Override
    public UUID createAssociationType( AssociationType associationType, UUID entityTypeId ) {
        final AssociationType existing = associationTypes.putIfAbsent( entityTypeId, associationType );

        if ( existing != null ) {
            logger.error(
                    "Inconsistency encountered in database. Verify that existing association types have all their acl keys reserved." );
        }

        eventBus.post( new AssociationTypeCreatedEvent( associationType ) );
        return entityTypeId;
    }

    @Override
    public AssociationType getAssociationType( UUID associationTypeId ) {
        AssociationType associationDetails = Preconditions.checkNotNull(
                Util.getSafely( associationTypes, associationTypeId ),
                "Association type of id %s does not exists.",
                associationTypeId.toString() );
        Optional<EntityType> entityType = Optional.fromNullable(
                Util.getSafely( entityTypes, associationTypeId ) );
        return new AssociationType(
                entityType,
                associationDetails.getSrc(),
                associationDetails.getDst(),
                associationDetails.isBidirectional() );
    }

    @Override
    public AssociationType getAssociationTypeSafe( UUID associationTypeId ) {
        Optional<AssociationType> associationDetails = Optional
                .fromNullable( Util.getSafely( associationTypes, associationTypeId ) );
        Optional<EntityType> entityType = Optional.fromNullable(
                Util.getSafely( entityTypes, associationTypeId ) );
        if ( !associationDetails.isPresent() || !entityType.isPresent() ) { return null; }
        return new AssociationType(
                entityType,
                associationDetails.get().getSrc(),
                associationDetails.get().getDst(),
                associationDetails.get().isBidirectional() );
    }

    @Override
    public void deleteAssociationType( UUID associationTypeId ) {
        AssociationType associationType = getAssociationType( associationTypeId );
        if ( associationType.getAssociationEntityType() == null ) {
            logger.error( "Inconsistency found: association type of id %s has no associated entity type",
                    associationTypeId );
            throw new IllegalStateException( "Failed to delete association type of id " + associationTypeId );
        }
        deleteEntityType( associationType.getAssociationEntityType().getId() );
        associationTypes.delete( associationTypeId );
        eventBus.post( new AssociationTypeDeletedEvent( associationTypeId ) );
    }

    @Override
    public AssociationDetails getAssociationDetails( UUID associationTypeId ) {
        AssociationType associationType = getAssociationType( associationTypeId );
        LinkedHashSet<EntityType> srcEntityTypes = associationType.getSrc()
                .stream()
                .map( entityTypeId -> getEntityType( entityTypeId ) )
                .collect( Collectors.toCollection( () -> new LinkedHashSet<>() ) );
        LinkedHashSet<EntityType> dstEntityTypes = associationType.getDst()
                .stream()
                .map( entityTypeId -> getEntityType( entityTypeId ) )
                .collect( Collectors.toCollection( () -> new LinkedHashSet<>() ) );
        return new AssociationDetails( srcEntityTypes, dstEntityTypes, associationType.isBidirectional() );
    }

    @Override
    public Iterable<EntityType> getAvailableAssociationTypesForEntityType( UUID entityTypeId ) {
        return entityTypeManager.getAssociationIdsForEntityType( entityTypeId ).map( id -> entityTypes.get( id ) )
                .collect( Collectors.toList() );
    }

    private void createOrUpdatePropertyTypeWithFqn( PropertyType pt, FullQualifiedName fqn ) {
        PropertyType existing = getPropertyType( pt.getId() );
        if ( existing == null ) { createPropertyTypeIfNotExists( pt ); } else {
            Optional<String> optionalTitleUpdate = ( pt.getTitle().equals( existing.getTitle() ) )
                    ? Optional.absent() : Optional.of( pt.getTitle() );
            Optional<String> optionalDescriptionUpdate = ( pt.getDescription().equals( existing.getDescription() ) )
                    ? Optional.absent() : Optional.of( pt.getDescription() );
            Optional<FullQualifiedName> optionalFqnUpdate = ( fqn.equals( existing.getType() ) )
                    ? Optional.absent() : Optional.of( fqn );
            Optional<Boolean> optionalPiiUpdate = ( pt.isPIIfield() == existing.isPIIfield() )
                    ? Optional.absent() : Optional.of( pt.isPIIfield() );
            updatePropertyTypeMetadata( existing.getId(), new MetadataUpdate(
                    optionalTitleUpdate,
                    optionalDescriptionUpdate,
                    Optional.absent(),
                    Optional.absent(),
                    optionalFqnUpdate,
                    optionalPiiUpdate,
                    Optional.absent(),
                    Optional.absent() ) );
        }
    }

    private void createOrUpdatePropertyType( PropertyType pt ) {
        createOrUpdatePropertyTypeWithFqn( pt, pt.getType() );
    }

    private void createOrUpdateEntityTypeWithFqn( EntityType et, FullQualifiedName fqn ) {
        EntityType existing = getEntityTypeSafe( et.getId() );
        if ( existing == null ) { createEntityType( et ); } else {
            Optional<String> optionalTitleUpdate = ( et.getTitle().equals( existing.getTitle() ) )
                    ? Optional.absent() : Optional.of( et.getTitle() );
            Optional<String> optionalDescriptionUpdate = ( et.getDescription().equals( existing.getDescription() ) )
                    ? Optional.absent() : Optional.of( et.getDescription() );
            Optional<FullQualifiedName> optionalFqnUpdate = ( fqn.equals( existing.getType() ) )
                    ? Optional.absent() : Optional.of( fqn );
            updateEntityTypeMetadata( existing.getId(), new MetadataUpdate(
                    optionalTitleUpdate,
                    optionalDescriptionUpdate,
                    Optional.absent(),
                    Optional.absent(),
                    optionalFqnUpdate,
                    Optional.absent(),
                    Optional.absent(),
                    Optional.absent() ) );
            if ( !et.getProperties().equals( existing.getProperties() ) ) {
                addPropertyTypesToEntityType( existing.getId(), et.getProperties() );
            }
        }
    }

    private void createOrUpdateEntityType( EntityType et ) {
        createOrUpdateEntityTypeWithFqn( et, et.getType() );
    }

    private void createOrUpdateAssociationTypeWithFqn( AssociationType at, FullQualifiedName fqn ) {
        EntityType et = at.getAssociationEntityType();
        AssociationType existing = getAssociationTypeSafe( et.getId() );
        if ( existing == null ) {
            createOrUpdateEntityTypeWithFqn( et, fqn );
            createAssociationType( at, getTypeAclKey( et.getType() ) );
        } else {
            if ( !existing.getSrc().equals( at.getSrc() ) ) {
                addSrcEntityTypesToAssociationType( existing.getAssociationEntityType().getId(), at.getSrc() );
            }
            if ( !existing.getDst().equals( at.getDst() ) ) {
                addDstEntityTypesToAssociationType( existing.getAssociationEntityType().getId(), at.getDst() );
            }
        }
    }

    private void createOrUpdateAssociationType( AssociationType at ) {
        createOrUpdateAssociationTypeWithFqn( at, at.getAssociationEntityType().getType() );
    }

    private void resolveFqnCycles(
            UUID id,
            SecurableObjectType objectType,
            Map<UUID, PropertyType> propertyTypesById,
            Map<UUID, EntityType> entityTypesById,
            Map<UUID, AssociationType> associationTypesById,
            boolean useTempFqn ) {
        FullQualifiedName tempFqn = new FullQualifiedName(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString() );
        switch ( objectType ) {
            case PropertyTypeInEntitySet:
                if ( useTempFqn ) { createOrUpdatePropertyTypeWithFqn( propertyTypesById.get( id ), tempFqn ); } else {
                    createOrUpdatePropertyType( propertyTypesById.get( id ) );
                }
                break;
            case EntityType:
                if ( useTempFqn ) { createOrUpdateEntityTypeWithFqn( entityTypesById.get( id ), tempFqn ); } else {
                    createOrUpdateEntityType( entityTypesById.get( id ) );
                }
                break;
            case AssociationType:
                if ( useTempFqn ) {
                    createOrUpdateAssociationTypeWithFqn( associationTypesById.get( id ), tempFqn );
                } else { createOrUpdateAssociationType( associationTypesById.get( id ) ); }
                break;
            default:
                break;
        }
    }

    @Override
    public void setEntityDataModel( EntityDataModel edm ) {
        Pair<EntityDataModelDiff, Set<List<UUID>>> diffAndFqnCycles = getEntityDataModelDiffAndFqnLists( edm );
        EntityDataModelDiff diff = diffAndFqnCycles.getLeft();
        Set<List<UUID>> fqnCycles = diffAndFqnCycles.getRight();
        if ( diff.getConflicts().isPresent() ) {
            throw new IllegalArgumentException(
                    "Unable to update entity data model: please resolve conflicts before importing." );
        }

        Map<UUID, SecurableObjectType> idToType = Maps.newHashMap();
        Map<UUID, PropertyType> propertyTypesById = Maps.newHashMap();
        Map<UUID, EntityType> entityTypesById = Maps.newHashMap();
        Map<UUID, AssociationType> associationTypesById = Maps.newHashMap();


        diff.getDiff().getPropertyTypes().forEach( pt -> {
            idToType.put( pt.getId(), SecurableObjectType.PropertyTypeInEntitySet );
            propertyTypesById.put( pt.getId(), pt );
        } );
        diff.getDiff().getEntityTypes().forEach( et -> {
            idToType.put( et.getId(), SecurableObjectType.EntityType );
            entityTypesById.put( et.getId(), et );
        } );
        diff.getDiff().getAssociationTypes().forEach( at -> {
            idToType.put( at.getAssociationEntityType().getId(), SecurableObjectType.AssociationType );
            associationTypesById.put( at.getAssociationEntityType().getId(), at );
        } );

        Set<UUID> updatedIds = Sets.newHashSet();

        fqnCycles.forEach( cycle -> {
            cycle.forEach( id -> {
                resolveFqnCycles( id,
                        idToType.get( id ),
                        propertyTypesById,
                        entityTypesById,
                        associationTypesById,
                        true );
            } );
            cycle.forEach( id -> {
                resolveFqnCycles( id,
                        idToType.get( id ),
                        propertyTypesById,
                        entityTypesById,
                        associationTypesById,
                        false );
                updatedIds.add( id );
            } );

        } );

        diff.getDiff().getSchemas().forEach( schema -> {
            schemaManager.createOrUpdateSchemas( schema );
        } );

        diff.getDiff().getPropertyTypes().forEach( pt -> {
            if ( !updatedIds.contains( pt.getId() ) ) { createOrUpdatePropertyType( pt ); }
        } );

        diff.getDiff().getEntityTypes().forEach( et -> {
            if ( !updatedIds.contains( et.getId() ) ) { createOrUpdateEntityType( et ); }
        } );

        diff.getDiff().getAssociationTypes().forEach( at -> {
            if ( !updatedIds.contains( at.getAssociationEntityType().getId() ) ) {
                createOrUpdateAssociationType( at );
            }
        } );
    }

    @Override
    public EntityDataModelDiff getEntityDataModelDiff( EntityDataModel edm ) {
        return getEntityDataModelDiffAndFqnLists( edm ).getLeft();
    }

    private Pair<EntityDataModelDiff, Set<List<UUID>>> getEntityDataModelDiffAndFqnLists( EntityDataModel edm ) {
        UUID currentVersion = getCurrentEntityDataModelVersion();
        if ( !edm.getVersion().equals( currentVersion ) ) {
            throw new IllegalArgumentException(
                    "Unable to generate diff: version " + edm.getVersion().toString()
                            + " does not match current version "
                            + currentVersion.toString() );
        }

        ConcurrentSkipListSet<PropertyType> conflictingPropertyTypes = new ConcurrentSkipListSet<>( Comparator
                .comparing( propertyType -> propertyType.getType().toString() ) );
        ConcurrentSkipListSet<EntityType> conflictingEntityTypes = new ConcurrentSkipListSet<>( Comparator
                .comparing( entityType -> entityType.getType().toString() ) );
        ConcurrentSkipListSet<AssociationType> conflictingAssociationTypes = new ConcurrentSkipListSet<>( Comparator
                .comparing( associationType -> associationType.getAssociationEntityType().getType().toString() ) );

        ConcurrentSkipListSet<PropertyType> updatedPropertyTypes = new ConcurrentSkipListSet<>( Comparator
                .comparing( propertyType -> propertyType.getType().toString() ) );
        ConcurrentSkipListSet<EntityType> updatedEntityTypes = new ConcurrentSkipListSet<>( Comparator
                .comparing( entityType -> entityType.getType().toString() ) );
        ConcurrentSkipListSet<AssociationType> updatedAssociationTypes = new ConcurrentSkipListSet<>( Comparator
                .comparing( associationType -> associationType.getAssociationEntityType().getType().toString() ) );
        ConcurrentSkipListSet<Schema> updatedSchemas = new ConcurrentSkipListSet<>( Comparator
                .comparing( schema -> schema.getFqn().toString() ) );

        Map<UUID, FullQualifiedName> idsToFqns = Maps.newHashMap();
        Map<UUID, SecurableObjectType> idsToTypes = Maps.newHashMap();
        Map<UUID, PropertyType> propertyTypesById = Maps.newHashMap();
        Map<UUID, EntityType> entityTypesById = Maps.newHashMap();
        Map<UUID, AssociationType> associationTypesById = Maps.newHashMap();

        edm.getPropertyTypes().forEach( pt -> {
            PropertyType existing = getPropertyType( pt.getId() );
            if ( existing == null ) { updatedPropertyTypes.add( pt ); } else if ( !existing.equals( pt ) ) {
                if ( !pt.getDatatype().equals( existing.getDatatype() )
                        || !pt.getAnalyzer().equals( existing.getAnalyzer() ) ) {
                    conflictingPropertyTypes.add( pt );
                } else if ( !pt.getType().equals( existing.getType() ) ) {
                    idsToTypes.put( pt.getId(), SecurableObjectType.PropertyTypeInEntitySet );
                    idsToFqns.put( pt.getId(), pt.getType() );
                    propertyTypesById.put( pt.getId(), pt );
                } else if ( !pt.getTitle().equals( existing.getTitle() )
                        || !pt.getDescription().equals( existing.getDescription() )
                        || !pt.isPIIfield() == existing.isPIIfield() ) { updatedPropertyTypes.add( pt ); }
            }
        } );

        edm.getEntityTypes().forEach( et -> {
            EntityType existing = getEntityTypeSafe( et.getId() );
            if ( existing == null ) { updatedEntityTypes.add( et ); } else if ( !existing.equals( et ) ) {
                if ( !et.getBaseType().equals( existing.getBaseType() )
                        || !et.getCategory().equals( existing.getCategory() )
                        || !et.getKey().equals( existing.getKey() ) ) {
                    conflictingEntityTypes.add( et );
                } else if ( !et.getType().equals( existing.getType() ) ) {
                    idsToTypes.put( et.getId(), SecurableObjectType.EntityType );
                    idsToFqns.put( et.getId(), et.getType() );
                    entityTypesById.put( et.getId(), et );
                } else if ( !et.getTitle().equals( existing.getTitle() )
                        || !et.getDescription().equals( existing.getDescription() )
                        || !et.getProperties().equals( existing.getProperties() ) ) { updatedEntityTypes.add( et ); }
            }
        } );

        edm.getAssociationTypes().forEach( at -> {
            EntityType atEntityType = at.getAssociationEntityType();
            AssociationType existing = getAssociationTypeSafe( atEntityType.getId() );
            if ( existing == null ) { updatedAssociationTypes.add( at ); } else if ( !existing.equals( at ) ) {
                if ( !at.isBidirectional() == existing.isBidirectional()
                        || !atEntityType.getBaseType().equals( existing.getAssociationEntityType().getBaseType() )
                        || !atEntityType.getCategory().equals( existing.getAssociationEntityType().getCategory() )
                        || !atEntityType.getKey().equals( existing.getAssociationEntityType().getKey() ) ) {
                    conflictingAssociationTypes.add( at );
                } else if ( !atEntityType.getType().equals( existing.getAssociationEntityType().getType() ) ) {
                    idsToTypes.put( atEntityType.getId(), SecurableObjectType.AssociationType );
                    idsToFqns.put( atEntityType.getId(), atEntityType.getType() );
                    associationTypesById.put( atEntityType.getId(), at );
                } else if ( !atEntityType.getTitle().equals( existing.getAssociationEntityType().getTitle() )
                        || !atEntityType.getDescription().equals( existing.getAssociationEntityType().getDescription() )
                        || !atEntityType.getProperties().equals( existing.getAssociationEntityType().getProperties() )
                        || !at.getSrc().equals( existing.getSrc() )
                        || !at.getDst().equals( existing.getDst() ) ) { updatedAssociationTypes.add( at ); }
            }
        } );
        edm.getSchemas().forEach( schema -> {
            Schema existing = null;
            if ( schemaManager.checkSchemaExists( schema.getFqn() ) ) {
                existing = schemaManager.getSchema( schema.getFqn().getNamespace(), schema.getFqn().getName() );
            }
            if ( existing == null || !schema.equals( existing ) ) { updatedSchemas.add( schema ); }
        } );

        List<Set<List<UUID>>> cyclesAndConflicts = checkFqnDiffs( idsToFqns );
        Map<UUID, Boolean> idsToOutcome = Maps.newHashMap();
        cyclesAndConflicts.get( 0 ).forEach( idList -> idList.forEach( id -> idsToOutcome.put( id, true ) ) );
        cyclesAndConflicts.get( 1 ).forEach( idList -> idList.forEach( id -> idsToOutcome.put( id, false ) ) );

        idsToOutcome.entrySet().forEach( idAndResolve -> {
            UUID id = idAndResolve.getKey();
            boolean shouldResolve = idAndResolve.getValue();
            switch ( idsToTypes.get( id ) ) {
                case PropertyTypeInEntitySet:
                    if ( shouldResolve ) { updatedPropertyTypes.add( propertyTypesById.get( id ) ); } else {
                        conflictingPropertyTypes.add( propertyTypesById.get( id ) );
                    }
                    break;
                case EntityType:
                    if ( shouldResolve ) { updatedEntityTypes.add( entityTypesById.get( id ) ); } else {
                        conflictingEntityTypes.add( entityTypesById.get( id ) );
                    }
                    break;
                case AssociationType:
                    if ( shouldResolve ) { updatedAssociationTypes.add( associationTypesById.get( id ) ); } else {
                        conflictingAssociationTypes.add( associationTypesById.get( id ) );
                    }
                    break;
                default:
                    break;
            }
        } );

        EntityDataModel edmDiff = new EntityDataModel(
                getCurrentEntityDataModelVersion(),
                Sets.newHashSet(),
                updatedSchemas,
                updatedEntityTypes,
                updatedAssociationTypes,
                updatedPropertyTypes );

        EntityDataModel conflicts = null;

        if ( !conflictingPropertyTypes.isEmpty() || !conflictingEntityTypes.isEmpty()
                || !conflictingAssociationTypes.isEmpty() ) {
            conflicts = new EntityDataModel(
                    getCurrentEntityDataModelVersion(),
                    Sets.newHashSet(),
                    Sets.newHashSet(),
                    conflictingEntityTypes,
                    conflictingAssociationTypes,
                    conflictingPropertyTypes );
        }

        EntityDataModelDiff diff = new EntityDataModelDiff( edmDiff, Optional.fromNullable( conflicts ) );
        Set<List<UUID>> cycles = cyclesAndConflicts.get( 0 );
        return Pair.of( diff, cycles );

    }

    private List<Set<List<UUID>>> checkFqnDiffs( Map<UUID, FullQualifiedName> idToType ) {
        Set<UUID> conflictingIdsToFqns = Sets.newHashSet();
        Map<UUID, FullQualifiedName> updatedIdToFqn = Maps.newHashMap();
        SetMultimap<FullQualifiedName, UUID> internalFqnToId = HashMultimap.create();
        Map<FullQualifiedName, UUID> externalFqnToId = Maps.newHashMap();

        idToType.entrySet().forEach( entry -> {
            UUID id = entry.getKey();
            FullQualifiedName fqn = entry.getValue();

            UUID conflictId = aclKeys.get( fqn.toString() );
            updatedIdToFqn.put( id, fqn );
            internalFqnToId.put( fqn, id );
            conflictingIdsToFqns.add( id );
            if ( conflictId != null ) { externalFqnToId.put( fqn, conflictId ); }
        } );

        return resolveFqnCyclesLists( conflictingIdsToFqns, updatedIdToFqn, internalFqnToId, externalFqnToId );
    }

    private List<Set<List<UUID>>> resolveFqnCyclesLists(
            Set<UUID> conflictingIdsToFqns,
            Map<UUID, FullQualifiedName> updatedIdToFqn,
            SetMultimap<FullQualifiedName, UUID> internalFqnToId,
            Map<FullQualifiedName, UUID> externalFqnToId ) {

        Set<List<UUID>> result = Sets.newHashSet();
        Set<List<UUID>> conflicts = Sets.newHashSet();

        while ( !conflictingIdsToFqns.isEmpty() ) {
            UUID initialId = conflictingIdsToFqns.iterator().next();
            List<UUID> conflictingIdsViewed = Lists.newArrayList();

            UUID id = initialId;

            boolean shouldReject = false;
            boolean shouldResolve = false;
            while ( !shouldReject && !shouldResolve ) {
                conflictingIdsViewed.add( 0, id );
                FullQualifiedName fqn = updatedIdToFqn.get( id );
                Set<UUID> idsForFqn = internalFqnToId.get( fqn );
                if ( idsForFqn.size() > 1 ) { shouldReject = true; } else {
                    id = externalFqnToId.get( fqn );
                    if ( id == null || id.equals( initialId ) ) { shouldResolve = true; } else if ( !updatedIdToFqn
                            .containsKey( id ) ) { shouldReject = true; }
                }
            }

            if ( shouldReject ) { conflicts.add( conflictingIdsViewed ); } else { result.add( conflictingIdsViewed ); }
            conflictingIdsToFqns.removeAll( conflictingIdsViewed );
        }
        return Lists.newArrayList( result, conflicts );
    }

    @Override
    public Map<UUID, EntitySetPropertyMetadata> getAllEntitySetPropertyMetadata(
            UUID entitySetId,
            Set<UUID> authorizedPropertyTypes ) {
        return authorizedPropertyTypes.stream()
                .collect( Collectors.toMap( propertyTypeId -> propertyTypeId,
                        propertyTypeId -> getEntitySetPropertyMetadata( entitySetId, propertyTypeId ) ) );
    }

    @Override
    public EntitySetPropertyMetadata getEntitySetPropertyMetadata( UUID entitySetId, UUID propertyTypeId ) {
        EntitySetPropertyKey key = new EntitySetPropertyKey( entitySetId, propertyTypeId );
        if ( !entitySetPropertyMetadata.containsKey( key ) ) {
            UUID entityTypeId = getEntitySet( entitySetId ).getEntityTypeId();
            setupDefaultEntitySetPropertyMetadata( entitySetId, entityTypeId );
        }
        return entitySetPropertyMetadata.get( key );
    }

    @Override
    public void updateEntitySetPropertyMetadata( UUID entitySetId, UUID propertyTypeId, MetadataUpdate update ) {
        EntitySetPropertyKey key = new EntitySetPropertyKey( entitySetId, propertyTypeId );
        entitySetPropertyMetadata.executeOnKey( key, new UpdateEntitySetPropertyMetadataProcessor( update ) );
    }

}
