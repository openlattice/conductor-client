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

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.HazelcastAclKeyReservationService;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.Principals;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.edm.events.EntitySetCreatedEvent;
import com.dataloom.edm.events.EntitySetDeletedEvent;
import com.dataloom.edm.events.EntitySetMetadataUpdatedEvent;
import com.dataloom.edm.events.PropertyTypesInEntitySetUpdatedEvent;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.edm.properties.CassandraTypeManager;
import com.dataloom.edm.schemas.manager.HazelcastSchemaManager;
import com.dataloom.edm.types.processors.AddPropertyTypesToEntityTypeProcessor;
import com.dataloom.edm.types.processors.RemovePropertyTypesFromEntityTypeProcessor;
import com.dataloom.edm.types.processors.RenameEntitySetProcessor;
import com.dataloom.edm.types.processors.RenameEntityTypeProcessor;
import com.dataloom.edm.types.processors.RenamePropertyTypeProcessor;
import com.dataloom.hazelcast.HazelcastMap;
import com.datastax.driver.core.Session;
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
    private final IMap<UUID, EntityType>            entityTypes;
    private final IMap<UUID, EntitySet>             entitySets;
    private final IMap<String, UUID>                aclKeys;
    private final IMap<UUID, String>                names;

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
        this.entityTypes = hazelcastInstance.getMap( HazelcastMap.ENTITY_TYPES.name() );
        this.entitySets = hazelcastInstance.getMap( HazelcastMap.ENTITY_SETS.name() );
        this.names = hazelcastInstance.getMap( HazelcastMap.NAMES.name() );
        this.aclKeys = hazelcastInstance.getMap( HazelcastMap.ACL_KEYS.name() );
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
        ensureValidPropertyType( propertyType );
        aclKeyReservations.reserveIdAndValidateType( propertyType );

        /*
         * Create property type if it doesn't exist. The reserveAclKeyAndValidateType call should ensure that
         */

        PropertyType dbRecord = propertyTypes.putIfAbsent( propertyType.getId(), propertyType );

        if ( dbRecord == null ) {
            propertyType.getSchemas().forEach( schemaManager.propertyTypesSchemaAdder( propertyType.getId() ) );
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
        }
    }

    public void createEntityType( EntityType entityType ) {
        /*
         * This is really create or replace and should be noted as such.
         */
        ensureValidEntityType( entityType );
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
        eventBus.post( new EntitySetDeletedEvent( entitySetId ) );
    }

    @Override
    public void assignEntityToEntitySet( UUID syncId, String entityId, String name ) {
        entitySetManager.assignEntityToEntitySet( syncId, entityId, name );
    }

    @Override
    public void assignEntityToEntitySet( UUID syncId, String entityId, EntitySet es ) {
        assignEntityToEntitySet( syncId, entityId, es.getName() );
    }

    private void createEntitySet( EntitySet entitySet ) {
        ensureValidEntitySet( entitySet );

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

            eventBus.post( new EntitySetCreatedEvent(
                    entitySet,
                    Lists.newArrayList( propertyTypes.getAll( ownablePropertyTypes ).values() ),
                    principal ) );

        } catch ( Exception e ) {
            logger.error( "Unable to create entity set {} for principal {}", entitySet, principal, e );
            Util.deleteSafely( entitySets, entitySet.getId() );
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
        entityTypes.executeOnKey( entityTypeId, new AddPropertyTypesToEntityTypeProcessor( propertyTypeIds ) );
    }

    @Override
    public void removePropertyTypesFromEntityType( UUID entityTypeId, Set<UUID> propertyTypeIds ) {
        Preconditions.checkArgument( checkPropertyTypesExist( propertyTypeIds ), "Some properties do not exist." );
        Preconditions.checkArgument(
                Sets.intersection( getEntityType( entityTypeId ).getKey(), propertyTypeIds ).isEmpty(),
                "Key property types cannot be removed." );
        entityTypes.executeOnKey( entityTypeId, new RemovePropertyTypesFromEntityTypeProcessor( propertyTypeIds ) );
    }

    @Override
    public void renameEntityType( UUID entityTypeId, FullQualifiedName newFqn ) {
        aclKeyReservations.renameReservation( entityTypeId, newFqn );
        entityTypes.executeOnKey( entityTypeId, new RenameEntityTypeProcessor( newFqn ) );
    }

    @Override
    public void renamePropertyType( UUID propertyTypeId, FullQualifiedName newFqn ) {
        aclKeyReservations.renameReservation( propertyTypeId, newFqn );
        propertyTypes.executeOnKey( propertyTypeId, new RenamePropertyTypeProcessor( newFqn ) );
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
    }

    @Override
    public void renameEntitySet( UUID entitySetId, String newName ) {
        aclKeyReservations.renameReservation( entitySetId, newName );
        entitySets.executeOnKey( entitySetId, new RenameEntitySetProcessor( newName ) );
        eventBus.post( new EntitySetMetadataUpdatedEvent( getEntitySet( entitySetId ) ) );
    }

    /**************
     * Validation
     **************/
    private void ensureValidEntityType( EntityType entityType ) {
        Preconditions.checkArgument( StringUtils.isNotBlank( entityType.getType().getNamespace() ),
                "Namespace for Entity Type is missing" );
        Preconditions.checkArgument( StringUtils.isNotBlank( entityType.getType().getName() ),
                "Name of Entity Type is missing" );
        Preconditions.checkArgument( StringUtils.isNotBlank( entityType.getTitle() ),
                "Title of Entity Type is missing" );
        Preconditions.checkArgument( CollectionUtils.isNotEmpty( entityType.getProperties() ),
                "Properties for Entity Type is missing" );
        Preconditions.checkArgument( CollectionUtils.isNotEmpty( entityType.getKey() ),
                "Key for Entity Type is missing" );
        Preconditions.checkArgument( checkPropertyTypesExist( entityType.getProperties() ),
                "Some properties do not exist" );
        Preconditions.checkArgument( entityType.getProperties().containsAll( entityType.getKey() ),
                "Properties must include all the key property types" );
    }

    private void ensureValidPropertyType( PropertyType propertyType ) {
        Preconditions.checkArgument( StringUtils.isNotBlank( propertyType.getType().getNamespace() ),
                "Namespace for Property Type is missing" );
        Preconditions.checkArgument( StringUtils.isNotBlank( propertyType.getType().getName() ),
                "Name of Property Type is missing" );
        Preconditions.checkArgument( StringUtils.isNotBlank( propertyType.getTitle() ),
                "Title of Property Type is missing" );
        Preconditions.checkArgument( propertyType.getDatatype() != null, "Datatype of Property Type is missing" );
    }

    private void ensureValidEntitySet( EntitySet entitySet ) {
        Preconditions.checkArgument( StringUtils.isNotBlank( entitySet.getName() ),
                "Name of Entity Set is missing." );
        Preconditions.checkArgument( StringUtils.isNotBlank( entitySet.getTitle() ),
                "Title of Entity Set is missing." );
        Preconditions.checkArgument( entitySet.getEntityTypeId() != null,
                "Entity Type Id of Entity Set is missing." );
        Preconditions.checkArgument( checkEntityTypeExists( entitySet.getEntityTypeId() ),
                "Entity Set Type does not exist." );
    }

    @Override
    public boolean checkPropertyTypesExist( Set<UUID> properties ) {
        return properties.stream().allMatch( propertyTypes::containsKey );
    }

    @Override
    public boolean checkPropertyTypeExists( UUID propertyTypeId ) {
        return propertyTypes.containsKey( propertyTypeId );
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
    
}
