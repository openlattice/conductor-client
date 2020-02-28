/*
 * Copyright (C) 2019. OpenLattice, Inc.
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
 *
 */
package com.openlattice.authorization

import com.codahale.metrics.annotation.Timed
import com.google.common.annotations.VisibleForTesting
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import org.springframework.stereotype.Component
import java.util.*
import java.util.stream.Collectors

@Component
class EdmAuthorizationHelper(
        private val edm: EdmManager,
        private val authz: AuthorizationManager,
        private val entitySetManager: EntitySetManager
) : AuthorizingComponent {
    companion object {
        @JvmField
        val READ_PERMISSION: EnumSet<Permission> = EnumSet.of(Permission.READ)

        @JvmField
        val WRITE_PERMISSION: EnumSet<Permission> = EnumSet.of(Permission.WRITE)

        @JvmField
        val OWNER_PERMISSION: EnumSet<Permission> = EnumSet.of(Permission.OWNER)

        @JvmStatic
        fun aclKeysForAccessCheck(
                rawAclKeys: Map<UUID, Set<UUID>>, requiredPermission: EnumSet<Permission>
        ): Map<AclKey, EnumSet<Permission>> {
            return rawAclKeys.flatMap { (key, values) ->
                values.map { value -> AclKey(key, value) to requiredPermission }
            }.toMap()
        }
    }

    /**
     * Get all property types of an entity set
     *
     * @param entitySetId the id of the entity set
     * @return all the property type ids on the entity type of the entity set
     */
    @Timed
    fun getAllPropertiesOnEntitySet(entitySetId: UUID): Set<UUID> {
        return entitySetManager.getEntityTypeByEntitySetId(entitySetId).properties
    }

    /**
     * @see [getAuthorizedEntitySetsForPrincipals]
     */
    @Timed
    fun getAuthorizedEntitySets(entitySetIds: Set<UUID>, requiredPermissions: EnumSet<Permission>): Set<UUID> {
        return getAuthorizedEntitySetsForPrincipals(
                entitySetIds, requiredPermissions, Principals.getCurrentPrincipals()
        )
    }

    /**
     * Returns the sub-set of the requested entity set ids, which are authorized.
     */
    @Timed
    fun getAuthorizedEntitySetsForPrincipals(
            entitySetIds: Set<UUID>, requiredPermissions: EnumSet<Permission>, principals: Set<Principal>
    ): Set<UUID> {
        return entitySetIds
                .filter { entitySetId ->
                    val entitySet = entitySetManager.getEntitySet(entitySetId)!!
                    val entitySetIdsToCheck = mutableSetOf(entitySetId)
                    if (entitySet.isLinking) {
                        entitySetIdsToCheck.addAll(entitySet.linkedEntitySets)
                    }

                    entitySetIdsToCheck.all { esId ->
                        authz.checkIfHasPermissions(AclKey(esId), principals, requiredPermissions)
                    }
                }.toSet()
    }

    /**
     * Collects the authorized property types mapped by the requested entity sets. For normal entity sets it does the
     * general checks for each of them and returns only those property types, where it has the required permissions.
     * For linking entity sets it return only those property types, where the calling user has the required permissions
     * in all of the normal entity sets.
     * Note: The returned maps keys are the requested entity set ids and not the normal entity set ids for linking
     * entity sets!
     *
     * @param entitySetIds        The entity set ids for which to get the authorized property types.
     * @param requiredPermissions The set of required permissions to check for.
     * @param principals          The set of pricipals to check the permissions against.
     * @return A Map with keys for each of the requested entity set id and values of authorized property types by their
     * id.
     */
    @Timed
    fun getAuthorizedPropertiesOnEntitySets(
            entitySetIds: Set<UUID>, requiredPermissions: EnumSet<Permission>, principals: Set<Principal>
    ): Map<UUID, Map<UUID, PropertyType>> {
        return if (entitySetIds.isEmpty()) {
            mapOf()
        } else {
            entitySetIds.map { entitySetId ->
                entitySetId to getAuthorizedPropertyTypes(entitySetId, requiredPermissions, principals)
            }.toMap()
        }
    }

    @Timed
    fun getAuthorizedPropertiesOnEntitySets(
            entitySetIds: Set<UUID>, requiredPermissions: EnumSet<Permission>
    ): Map<UUID, Map<UUID, PropertyType>> {
        return getAuthorizedPropertiesOnEntitySets(entitySetIds, requiredPermissions, Principals.getCurrentPrincipals())
    }

    @Timed
    fun getAuthorizedPropertyTypes(
            entitySetId: UUID, requiredPermissions: EnumSet<Permission>
    ): Map<UUID, PropertyType> {
        return getAuthorizedPropertyTypes(entitySetId, requiredPermissions, Principals.getCurrentPrincipals())
    }

    @Timed
    fun getAuthorizedPropertyTypes(
            entitySetId: UUID, requiredPermissions: EnumSet<Permission>, principals: Set<Principal>
    ): Map<UUID, PropertyType> {
        val propertyTypes = entitySetManager.getPropertyTypesForEntitySet(entitySetId)

        return getAuthorizedPropertyTypes(entitySetId, requiredPermissions, propertyTypes, principals)
    }

    /**
     * Note: entitysets are assumed to have same entity type
     */
    @Timed
    fun getAuthorizedPropertyTypes(
            entitySetIds: Set<UUID>, selectedProperties: Set<UUID>, requiredPermissions: EnumSet<Permission>
    ): Map<UUID, Map<UUID, PropertyType>> {
        return getAuthorizedPropertyTypes(
                entitySetIds, selectedProperties, requiredPermissions, Principals.getCurrentPrincipals()
        )
    }

    /**
     * Note: entitysets are assumed to have same entity type
     */
    fun getAuthorizedPropertyTypes(
            entitySetIds: Set<UUID>,
            selectedProperties: Set<UUID>,
            requiredPermissions: EnumSet<Permission>,
            principals: Set<Principal>
    ): Map<UUID, Map<UUID, PropertyType>> {
        return entitySetIds.map {
            it to getAuthorizedPropertyTypes(
                    it, requiredPermissions, edm.getPropertyTypesAsMap(selectedProperties), principals
            )
        }.toMap()
    }

    @Timed
    fun getAuthorizedPropertyTypes(
            entitySetId: UUID,
            requiredPermissions: EnumSet<Permission>,
            propertyTypes: Map<UUID, PropertyType>,
            principals: Set<Principal>
    ): Map<UUID, PropertyType> {
        val entitySet = entitySetManager.getEntitySet(entitySetId)!!

        return if (entitySet.isLinking) {
            getAuthorizedPropertyTypesOfLinkingEntitySet(entitySet, propertyTypes.keys, requiredPermissions, principals)
        } else {
            getAuthorizedPropertyTypesOfNormalEntitySet(entitySetId, propertyTypes, requiredPermissions, principals)
        }
    }

    /**
     * @return Authorized property types for the requested permissions where at least 1 requested principal has been
     * authorization for.
     */
    @VisibleForTesting
    fun getAuthorizedPropertyTypesOfNormalEntitySet(
            entitySetId: UUID,
            propertyTypes: Map<UUID, PropertyType>,
            requiredPermissions: EnumSet<Permission>,
            principals: Set<Principal>
    ): Map<UUID, PropertyType> {

        val accessRequest = propertyTypes.keys.map { ptId -> AclKey(entitySetId, ptId) to requiredPermissions }.toMap()

        val authorizations = authorize(accessRequest, principals)
        return authorizations.entries
                .filter { authz -> authz.value.values.all { v -> v } }
                .map { authz -> authz.key[1] to propertyTypes.getValue(authz.key[1]) }
                .toMap()
    }

    private fun getAuthorizedPropertyTypesOfLinkingEntitySet(
            linkingEntitySet: EntitySet,
            propertyTypeIds: Set<UUID>,
            requiredPermissions: EnumSet<Permission>,
            principals: Set<Principal>
    ): Map<UUID, PropertyType> {
        if (linkingEntitySet.linkedEntitySets.isEmpty()) {
            return mapOf()
        }

        val propertyPermissions = getPermissionsOnLinkingEntitySetProperties(
                linkingEntitySet.linkedEntitySets, propertyTypeIds, principals
        )

        return propertyPermissions.entries
                .filter { entry -> entry.value.containsAll(requiredPermissions) }
                .map { it.key.id to it.key }
                .toMap()
    }

    @Timed
    fun getAuthorizedPropertyTypesByNormalEntitySet(
            linkingEntitySet: EntitySet, selectedProperties: Set<UUID>, requiredPermissions: EnumSet<Permission>
    ): Map<UUID, Map<UUID, PropertyType>> {
        return getAuthorizedPropertyTypesByNormalEntitySet(
                linkingEntitySet, selectedProperties, requiredPermissions, Principals.getCurrentPrincipals()
        )
    }

    private fun getAuthorizedPropertyTypesByNormalEntitySet(
            linkingEntitySet: EntitySet,
            selectedProperties: Set<UUID>,
            requiredPermissions: EnumSet<Permission>,
            principals: Set<Principal>
    ): Map<UUID, Map<UUID, PropertyType>> {
        if (linkingEntitySet.linkedEntitySets.isEmpty()) {
            return mapOf()
        }

        val propertyPermissions = getPermissionsOnLinkingEntitySetProperties(
                linkingEntitySet.linkedEntitySets, selectedProperties, principals
        )

        val authorizedProperties = propertyPermissions.entries
                .filter { (_, permissions) -> permissions.containsAll(requiredPermissions) }
                .map { (propertyType, _) -> propertyType.id to propertyType }
                .toMap()

        return linkingEntitySet.linkedEntitySets.map { it to authorizedProperties }.toMap()
    }


    @Timed
    fun getAuthorizedPropertyTypeIds(
            entitySetId: UUID, requiredPermissions: EnumSet<Permission>
    ): Set<UUID> {
        val entitySet = entitySetManager.getEntitySet(entitySetId)!!
        val properties = getAllPropertiesOnEntitySet(entitySetId)

        return if (entitySet.isLinking) {
            getAuthorizedPropertyTypeIdsOnLinkingEntitySet(entitySet, properties, requiredPermissions)
        } else {
            getAuthorizedPropertyTypeIdsOnNormalEntitySet(entitySetId, properties, requiredPermissions)
        }
    }

    private fun getAuthorizedPropertyTypeIdsOnNormalEntitySet(
            entitySetId: UUID, selectedProperties: Set<UUID>, requiredPermissions: EnumSet<Permission>
    ): Set<UUID> {
        return authz.accessChecksForPrincipals(
                selectedProperties.map { ptId ->
                    AccessCheck(AclKey(entitySetId, ptId), requiredPermissions)
                }.toSet(),
                Principals.getCurrentPrincipals()
        )
                .filter { authorization -> authorization.permissions.values.all { v -> v } }
                .map { authorization -> authorization.aclKey[1] }
                .collect(Collectors.toSet())
    }

    private fun getAuthorizedPropertyTypeIdsOnLinkingEntitySet(
            linkingEntitySet: EntitySet, selectedProperties: Set<UUID>, requiredPermissions: EnumSet<Permission>
    ): Set<UUID> {
        val propertyPermissions = getPermissionsOnLinkingEntitySetProperties(
                linkingEntitySet.linkedEntitySets, selectedProperties, Principals.getCurrentPrincipals()
        )

        return propertyPermissions.entries
                .filter { (_, permissions) -> permissions.containsAll(requiredPermissions) }
                .map { it.key.id }
                .toSet()
    }

    /**
     * @return the intersection of permissions for each provided property type id of the normal entity sets
     */
    private fun getPermissionsOnLinkingEntitySetProperties(
            entitySetIds: Set<UUID>, selectedProperties: Set<UUID>, principals: Set<Principal>
    ): Map<PropertyType, EnumSet<Permission>> {
        val propertyTypes = edm.getPropertyTypesAsMap(selectedProperties)

        val aclKeySets = propertyTypes.keys.map { ptId ->
            ptId to entitySetIds.map { esId -> AclKey(esId, ptId) }.toSet()
        }.toMap()

        val permissionsMap = authz.getSecurableObjectSetsPermissions(aclKeySets.values, principals)

        return propertyTypes.values.map {
            it to permissionsMap.getValue(aclKeySets.getValue(it.id))
        }.toMap()
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authz
    }
}