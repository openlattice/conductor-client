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

package com.kryptnostic.datastore.services;

import static com.google.common.base.Preconditions.checkState;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.HazelcastAclKeyReservationService;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.Principals;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.edm.EntityDataModel;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.Schema;
import com.dataloom.edm.events.AssociationTypeCreatedEvent;
import com.dataloom.edm.events.AssociationTypeDeletedEvent;
import com.dataloom.edm.events.EntitySetCreatedEvent;
import com.dataloom.edm.events.EntitySetDeletedEvent;
import com.dataloom.edm.events.EntitySetMetadataUpdatedEvent;
import com.dataloom.edm.events.EntityTypeCreatedEvent;
import com.dataloom.edm.events.EntityTypeDeletedEvent;
import com.dataloom.edm.events.PropertyTypeCreatedEvent;
import com.dataloom.edm.events.PropertyTypeDeletedEvent;
import com.dataloom.edm.events.PropertyTypesInEntitySetUpdatedEvent;
import com.dataloom.edm.exceptions.TypeNotFoundException;
import com.dataloom.edm.properties.CassandraTypeManager;
import com.dataloom.edm.requests.MetadataUpdate;
import com.dataloom.edm.schemas.manager.HazelcastSchemaManager;
import com.dataloom.edm.type.AssociationDetails;
import com.dataloom.edm.type.AssociationType;
import com.dataloom.edm.type.ComplexType;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.EnumType;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.edm.types.processors.AddDstEntityTypesToAssociationTypeProcessor;
import com.dataloom.edm.types.processors.AddPropertyTypesToEntityTypeProcessor;
import com.dataloom.edm.types.processors.AddSrcEntityTypesToAssociationTypeProcessor;
import com.dataloom.edm.types.processors.RemoveDstEntityTypesFromAssociationTypeProcessor;
import com.dataloom.edm.types.processors.RemovePropertyTypesFromEntityTypeProcessor;
import com.dataloom.edm.types.processors.RemoveSrcEntityTypesFromAssociationTypeProcessor;
import com.dataloom.edm.types.processors.ReorderPropertyTypesInEntityTypeProcessor;
import com.dataloom.edm.types.processors.UpdateEntitySetMetadataProcessor;
import com.dataloom.edm.types.processors.UpdateEntityTypeMetadataProcessor;
import com.dataloom.edm.types.processors.UpdatePropertyTypeMetadataProcessor;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.hazelcast.HazelcastUtils;
import com.datastax.driver.core.Session;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.EntryProcessor;
import com.kryptnostic.datastore.util.Util;

public class EdmService implements EdmManager {

    private static final Logger                     logger = LoggerFactory.getLogger( EdmService.class );
    private final IMap<UUID, PropertyType>          propertyTypes;
    private final IMap<UUID, ComplexType>           complexTypes;
    private final IMap<UUID, EnumType>              enumTypes;
    private final IMap<UUID, EntityType>            entityTypes;
    private final IMap<UUID, EntitySet>             entitySets;
    private final IMap<String, UUID>                aclKeys;
    private final IMap<UUID, String>                names;
    private final IMap<UUID, AssociationType>       associationTypes;
    private final IMap<UUID, UUID>                  syncIds;

    private final HazelcastAclKeyReservationService aclKeyReservations;
    private final AuthorizationManager              authorizations;
    private final CassandraEntitySetManager         entitySetManager;
    private final CassandraTypeManager              entityTypeManager;
    private final HazelcastSchemaManager            schemaManager;

    @Inject
    private EventBus                                eventBus;

    public EdmService(
            String keyspace,
            Session session,
            HazelcastInstance hazelcastInstance,
            HazelcastAclKeyReservationService aclKeyReservations,
            AuthorizationManager authorizations,
            CassandraEntitySetManager entitySetManager,
            CassandraTypeManager entityTypeManager,
            HazelcastSchemaManager schemaManager ) {
        this.authorizations = authorizations;
        this.entitySetManager = entitySetManager;
        this.entityTypeManager = entityTypeManager;
        this.schemaManager = schemaManager;
        this.propertyTypes = hazelcastInstance.getMap( HazelcastMap.PROPERTY_TYPES.name() );
        this.complexTypes = hazelcastInstance.getMap( HazelcastMap.COMPLEX_TYPES.name() );
        this.enumTypes = hazelcastInstance.getMap( HazelcastMap.ENUM_TYPES.name() );
        ;
        this.entityTypes = hazelcastInstance.getMap( HazelcastMap.ENTITY_TYPES.name() );
        this.entitySets = hazelcastInstance.getMap( HazelcastMap.ENTITY_SETS.name() );
        this.names = hazelcastInstance.getMap( HazelcastMap.NAMES.name() );
        this.aclKeys = hazelcastInstance.getMap( HazelcastMap.ACL_KEYS.name() );
        this.associationTypes = hazelcastInstance.getMap( HazelcastMap.ASSOCIATION_TYPES.name() );
        this.syncIds = hazelcastInstance.getMap( HazelcastMap.SYNC_IDS.name() );
        this.aclKeyReservations = aclKeyReservations;
        entityTypes.values().forEach( entityType -> logger.debug( "Object type read: {}", entityType ) );
        propertyTypes.values().forEach( propertyType -> logger.debug( "Property type read: {}", propertyType ) );
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.types.services.EdmManager#createPropertyType(com.kryptnostic.types.PropertyType) update
     * propertyType (and return true upon success) if exists, return false otherwise
     */
    @Override
    public void createPropertyTypeIfNotExists( PropertyType propertyType ) {
        aclKeyReservations.reserveIdAndValidateType( propertyType );

        /*
         * Create property type if it doesn't exist. The reserveAclKeyAndValidateType call should ensure that
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
            propertyTypes.delete( propertyTypeId );
            aclKeyReservations.release( propertyTypeId );
            eventBus.post( new PropertyTypeDeletedEvent( propertyTypeId ) );
        } else {
            throw new IllegalArgumentException(
                    "Unable to delete property type because it is associated with an entity set." );
        }
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
        authorizations.deletePermissions( ImmutableList.of( entitySetId ) );
        entityType.getProperties().stream()
                .map( propertyTypeId -> ImmutableList.of( entitySetId, propertyTypeId ) )
                .forEach( authorizations::deletePermissions );

        Util.deleteSafely( entitySets, entitySetId );
        aclKeyReservations.release( entitySetId );
        syncIds.remove( entitySetId );
        eventBus.post( new EntitySetDeletedEvent( entitySetId ) );
    }

    private void createEntitySet( EntitySet entitySet ) {
        aclKeyReservations.reserveIdAndValidateType( entitySet );

        checkState( entitySets.putIfAbsent( entitySet.getId(), entitySet ) == null, "Entity set already exists." );
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
            authorizations.addPermission( ImmutableList.of( entitySet.getId() ),
                    principal,
                    EnumSet.allOf( Permission.class ) );

            authorizations.createEmptyAcl( ImmutableList.of( entitySet.getId() ), SecurableObjectType.EntitySet );

            ownablePropertyTypes.stream()
                    .map( propertyTypeId -> ImmutableList.of( entitySet.getId(), propertyTypeId ) )
                    .peek( aclKey -> authorizations.addPermission(
                            aclKey,
                            principal,
                            EnumSet.allOf( Permission.class ) ) )
                    .forEach( aclKey -> authorizations.createEmptyAcl( aclKey,
                            SecurableObjectType.PropertyTypeInEntitySet ) );

            if ( !getEntityType( entitySet.getEntityTypeId() ).getCategory()
                    .equals( SecurableObjectType.AssociationType ) ) {
                eventBus.post( new EntitySetCreatedEvent(
                        entitySet,
                        Lists.newArrayList( propertyTypes.getAll( ownablePropertyTypes ).values() ),
                        principal ) );
            }

        } catch ( Exception e ) {
            logger.error( "Unable to create entity set {} for principal {}", entitySet, principal, e );
            Util.deleteSafely( entitySets, entitySet.getId() );
            aclKeyReservations.release( entitySet.getId() );
            throw new IllegalStateException( "Unable to create entity set: " + entitySet.getId() );
        }
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
                "Entity type %s does not exist.",
                typeFqn.getFullQualifiedNameAsString() );
        return getEntityType( entityTypeId );
    }

    @Override
    public EntityType getEntityType( UUID entityTypeId ) {
        return Preconditions.checkNotNull(
                Util.getSafely( entityTypes, entityTypeId ),
                "Entity type of id %s does not exist.",
                entityTypeId.toString() );
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
                "Property type %s does not exist",
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
        Preconditions.checkArgument( checkPropertyTypesExist( propertyTypeIds ), "Some properties do not exist." );
        Stream<UUID> childrenIds = entityTypeManager.getEntityTypeChildrenIdsDeep( entityTypeId );
        Map<UUID, Boolean> childrenIdsToLocks = childrenIds
                .collect( Collectors.toMap( Functions.<UUID> identity()::apply, propertyTypes::tryLock ) );
        childrenIdsToLocks.values().forEach( locked -> {
            if ( !locked ) {
                childrenIdsToLocks.entrySet().forEach( entry -> {
                    if ( entry.getValue() ) propertyTypes.unlock( entry.getKey() );
                } );
                throw new IllegalStateException(
                        "Unable to modify the entity data model right now--please try again." );
            }
        } );
        childrenIdsToLocks.keySet().forEach( id -> {
            entityTypes.executeOnKey( id, new AddPropertyTypesToEntityTypeProcessor( propertyTypeIds ) );

            for ( EntitySet entitySet : entitySetManager.getAllEntitySetsForType( id ) ) {
                UUID esId = entitySet.getId();
                Iterable<Principal> owners = authorizations.getSecurableObjectOwners( Arrays.asList( esId ) );
                for ( Principal owner : owners ) {
                    propertyTypeIds.stream()
                            .map( propertyTypeId -> ImmutableList.of( entitySet.getId(), propertyTypeId ) )
                            .forEach( aclKey -> {
                                authorizations.addPermission(
                                        aclKey,
                                        owner,
                                        EnumSet.allOf( Permission.class ) );
                                authorizations.createEmptyAcl( aclKey,
                                        SecurableObjectType.PropertyTypeInEntitySet );
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
            if ( entry.getValue() ) propertyTypes.unlock( entry.getKey() );
        } );
    }

    @Override
    public void removePropertyTypesFromEntityType( UUID entityTypeId, Set<UUID> propertyTypeIds ) {
        Preconditions.checkArgument( checkPropertyTypesExist( propertyTypeIds ), "Some properties do not exist." );
        EntityType entityType = getEntityType( entityTypeId );
        if ( entityType.getBaseType().isPresent() ) {
            EntityType baseType = getEntityType( entityType.getBaseType().get() );
            Preconditions.checkArgument( Sets.intersection( propertyTypeIds, baseType.getProperties() ).isEmpty(),
                    "Inherited property types cannot be removed." );
        }

        List<UUID> childrenIds = entityTypeManager.getEntityTypeChildrenIdsDeep( entityTypeId )
                .collect( Collectors.<UUID> toList() );
        childrenIds.forEach( id -> {
            Preconditions.checkArgument( Sets.intersection( getEntityType( id ).getKey(), propertyTypeIds ).isEmpty(),
                    "Key property types cannot be removed." );
            Preconditions.checkArgument( !entitySetManager.getAllEntitySetsForType( id ).iterator().hasNext(),
                    "Property types cannot be removed from entity types that have already been associated with an entity set." );
        } );
        Map<UUID, Boolean> childrenIdsToLocks = childrenIds.stream()
                .collect( Collectors.toMap( Functions.<UUID> identity()::apply, propertyTypes::tryLock ) );
        childrenIdsToLocks.values().forEach( locked -> {
            if ( !locked ) {
                childrenIdsToLocks.entrySet().forEach( entry -> {
                    if ( entry.getValue() ) propertyTypes.unlock( entry.getKey() );
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
                "Association type of id %s does not exist.",
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

    private void updateEntityType( EntityType et, EntityType existing ) {
        Optional<String> optionalTitleUpdate = ( et.getTitle().equals( existing.getTitle() ) )
                ? Optional.absent() : Optional.of( et.getTitle() );
        Optional<String> optionalDescriptionUpdate = ( et.getDescription().equals( existing.getDescription() ) )
                ? Optional.absent() : Optional.of( et.getDescription() );
        Optional<FullQualifiedName> optionalFqnUpdate = ( et.getType().equals( existing.getType() ) )
                ? Optional.absent() : Optional.of( et.getType() );
        updateEntityTypeMetadata( existing.getId(), new MetadataUpdate(
                optionalTitleUpdate,
                optionalDescriptionUpdate,
                Optional.absent(),
                Optional.absent(),
                optionalFqnUpdate,
                Optional.absent() ) );
        if ( !et.getProperties().equals( existing.getProperties() ) )
            addPropertyTypesToEntityType( existing.getId(), et.getProperties() );
    }

    @Override
    public void setEntityDataModel( EntityDataModel edm ) {
        edm.getPropertyTypes().forEach( pt -> {
            PropertyType existing = null;
            if ( pt.wasIdPresent() )
                existing = getPropertyType( pt.getId() );
            else {
                UUID id = getTypeAclKey( pt.getType() );
                if ( id != null ) existing = getPropertyType( id );
            }

            if ( existing == null )
                createPropertyTypeIfNotExists( pt );
            else {
                Optional<String> optionalTitleUpdate = ( pt.getTitle().equals( existing.getTitle() ) )
                        ? Optional.absent() : Optional.of( pt.getTitle() );
                Optional<String> optionalDescriptionUpdate = ( pt.getDescription().equals( existing.getDescription() ) )
                        ? Optional.absent() : Optional.of( pt.getDescription() );
                Optional<FullQualifiedName> optionalFqnUpdate = ( pt.getType().equals( existing.getType() ) )
                        ? Optional.absent() : Optional.of( pt.getType() );
                Optional<Boolean> optionalPiiUpdate = ( pt.isPIIfield() == existing.isPIIfield() )
                        ? Optional.absent() : Optional.of( pt.isPIIfield() );
                updatePropertyTypeMetadata( existing.getId(), new MetadataUpdate(
                        optionalTitleUpdate,
                        optionalDescriptionUpdate,
                        Optional.absent(),
                        Optional.absent(),
                        optionalFqnUpdate,
                        optionalPiiUpdate ) );
            }
        } );
        edm.getEntityTypes().forEach( et -> {
            EntityType existing = null;
            if ( et.wasIdPresent() )
                existing = getEntityType( et.getId() );
            else {
                UUID id = getTypeAclKey( et.getType() );
                if ( id != null ) existing = getEntityType( id );
            }
            if ( existing == null )
                createEntityType( et );
            else {
                updateEntityType( et, existing );
            }
        } );
        edm.getAssociationTypes().forEach( at -> {
            AssociationType existing = null;
            EntityType et = at.getAssociationEntityType();
            if ( et.wasIdPresent() )
                existing = getAssociationType( et.getId() );
            else {
                UUID id = getTypeAclKey( et.getType() );
                if ( id != null ) existing = getAssociationType( id );
            }
            if ( existing == null ) {
                createEntityType( et );
                createAssociationType( at, getTypeAclKey( et.getType() ) );
            } else {
                updateEntityType( et, existing.getAssociationEntityType() );
                if ( !existing.getSrc().equals( at.getSrc() ) )
                    addSrcEntityTypesToAssociationType( existing.getAssociationEntityType().getId(), at.getSrc() );
                if ( !existing.getDst().equals( at.getDst() ) )
                    addDstEntityTypesToAssociationType( existing.getAssociationEntityType().getId(), at.getDst() );
            }
        } );
        edm.getSchemas().forEach( schema -> {
            if ( schemaManager.checkSchemaExists( schema.getFqn() ) ) {
                Schema existing = schemaManager.getSchema( schema.getFqn().getNamespace(), schema.getFqn().getName() );
                if ( !existing.getEntityTypes().equals( schema.getEntityTypes() ) ) {
                    schemaManager.addEntityTypesToSchema( schema.getEntityTypes().stream().map( et -> {
                        return et.getId();
                    } ).collect( Collectors.toSet() ), schema.getFqn() );
                }
                if ( !existing.getPropertyTypes().equals( schema.getPropertyTypes() ) ) {
                    schemaManager.addPropertyTypesToSchema( schema.getPropertyTypes().stream().map( pt -> {
                        return pt.getId();
                    } ).collect( Collectors.toSet() ), schema.getFqn() );
                }
            } else {
            schemaManager.createOrUpdateSchemas( schema );
             }
        } );
    }

    @Override
    public EntityDataModel getEntityDataModelDiff( EntityDataModel edm ) {
        Set<PropertyType> updatedPropertyTypes = Sets.newHashSet();
        Set<EntityType> updatedEntityTypes = Sets.newHashSet();
        Set<AssociationType> updatedAssociationTypes = Sets.newHashSet();
        Set<Schema> updatedSchemas = Sets.newHashSet();
        edm.getPropertyTypes().forEach( pt -> {
            PropertyType existing = null;
            if ( pt.wasIdPresent() ) {
                if ( checkPropertyTypeExists( pt.getId() ) ) existing = getPropertyType( pt.getId() );
            } else {
                UUID id = getTypeAclKey( pt.getType() );
                if ( id != null ) existing = getPropertyType( id );
            }
            if ( existing == null
                    || !pt.getType().equals( existing.getType() )
                    || !pt.getTitle().equals( existing.getTitle() )
                    || !pt.getDescription().equals( existing.getDescription() )
                    || !pt.isPIIfield() == existing.isPIIfield() )
                updatedPropertyTypes.add( pt );
        } );

        edm.getEntityTypes().forEach( et -> {
            EntityType existing = null;
            if ( et.wasIdPresent() ) {
                if ( checkEntityTypeExists( et.getId() ) ) existing = getEntityType( et.getId() );
            } else {
                UUID id = getTypeAclKey( et.getType() );
                if ( id != null ) existing = getEntityType( id );
            }
            if ( existing == null
                    || !et.getType().equals( existing.getType() )
                    || !et.getTitle().equals( existing.getTitle() )
                    || !et.getDescription().equals( existing.getDescription() )
                    || !et.getProperties().equals( existing.getProperties() ) )
                updatedEntityTypes.add( et );
        } );

        edm.getAssociationTypes().forEach( at -> {
            AssociationType existing = null;
            if ( at.getAssociationEntityType().wasIdPresent() ) {
                if ( checkEntityTypeExists( at.getAssociationEntityType().getId() ) )
                    existing = getAssociationType( at.getAssociationEntityType().getId() );
            } else {
                UUID id = getTypeAclKey( at.getAssociationEntityType().getType() );
                if ( id != null ) existing = getAssociationType( id );
            }
            EntityType atEntityType = at.getAssociationEntityType();
            if ( existing == null
                    || !atEntityType.getType().equals( existing.getAssociationEntityType().getType() )
                    || !atEntityType.getTitle().equals( existing.getAssociationEntityType().getTitle() )
                    || !atEntityType.getDescription().equals( existing.getAssociationEntityType().getDescription() )
                    || !atEntityType.getProperties().equals( existing.getAssociationEntityType().getProperties() )
                    || !at.getSrc().equals( existing.getSrc() )
                    || !at.getDst().equals( existing.getDst() ) )
                updatedAssociationTypes.add( at );
        } );
        edm.getSchemas().forEach( schema -> {
            Schema existing = null;
            if ( schemaManager.checkSchemaExists( schema.getFqn() ) ) {
                existing = schemaManager.getSchema( schema.getFqn().getNamespace(), schema.getFqn().getName() );
            }
            if ( existing == null || !schema.equals( existing ) ) updatedSchemas.add( schema );
        } );

        return new EntityDataModel(
                Sets.newHashSet(),
                updatedSchemas,
                updatedEntityTypes,
                updatedAssociationTypes,
                updatedPropertyTypes );
    }

}
