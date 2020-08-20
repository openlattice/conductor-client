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
package com.openlattice.assembler

import com.hazelcast.map.IMap
import com.openlattice.authorization.DbCredentialService
import com.openlattice.authorization.EdmAuthorizationHelper
import com.openlattice.datastore.services.EdmManager
import com.openlattice.organization.OrganizationEntitySetFlag
import com.openlattice.organizations.HazelcastOrganizationService
import com.openlattice.tasks.HazelcastTaskDependencies
import com.zaxxer.hikari.HikariDataSource
import java.util.*

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface AssemblerConnectionManagerDependent<T> {
    companion object {
        const val NOT_INITIALIZED = "Assembler Connection Manager not initialized."
    }
    fun init(acm: AssemblerConnectionManager): T
}

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class AssemblerDependencies(
        val hds: HikariDataSource,
        val dbCredentialService: DbCredentialService,
        val assemblerConnectionManager: AssemblerConnectionManager
) : HazelcastTaskDependencies {
    val target: HikariDataSource = assemblerConnectionManager.connect("postgres")
}

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class OrganizationAssembly(
        val organizationId: UUID,
        var initialized : Boolean = false,
        val materializedEntitySets: MutableMap<UUID, EnumSet<OrganizationEntitySetFlag>> = mutableMapOf())

/**
 *
 */
data class MaterializedEntitySetsDependencies(
        val assembler: Assembler,
        val materializedEntitySets: IMap<EntitySetAssemblyKey, MaterializedEntitySet>,
        val organizations: HazelcastOrganizationService,
        val edm: EdmManager,
        val authzHelper: EdmAuthorizationHelper,
        val hds: HikariDataSource
) : HazelcastTaskDependencies
