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
 *
 */

package com.openlattice.graph

import com.google.common.base.Preconditions.checkState
import com.openlattice.analysis.requests.Filter
import com.openlattice.data.storage.MetadataOption
import com.openlattice.data.storage.selectEntitySetWithCurrentVersionOfPropertyTypes
import com.openlattice.edm.type.PropertyType
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import java.util.*

/**
 *
 * This class is used for building out the sql for querying a filtered subgraph. As edges don't exist on linked entities
 * if only
 *
 *
 * @param authorizedPropertyTypes
 * @param srcFilters The src filters grouped by entity type id and consisting of maps from entiy set id to maps from
 * property type id to sets of filters to optimize querying.
 *
 */

data class EdgesQuery(
        val authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
        val srcFilters: List<GraphEntityConstraint>,
        val dstFilters: List<GraphEntityConstraint>,
        val edgeFilters: List<GraphEntityConstraint>
) {

    private val flattenedAuthorizedPropertyTypes = authorizedPropertyTypes.flatMap { it.value.values }
            .toSet()
            .map { it.id to it }
            .toMap()

    fun sql(): String {
        TODO("Implement this function.")
    }

    fun buildQuery(): String {
        val srcQuery = srcFilters.map { this::buildGraphEntityQuery }.joinToString { " UNION " }
        val dstQuery = dstFilters.map { this::buildGraphEntityQuery }.joinToString { " UNION " }
        val edgeQuery = edgeFilters.map { this::buildGraphEntityQuery }.joinToString { " UNION " }

        return "SELECT * FROM ($srcQuery) as srcQuery, "
    }

    private fun buildGraphEntityQuery(constraint: GraphEntityConstraint): String {
        val filters = constraint.filters
        val entityKeyIds = filters.mapValues { Optional.empty<Set<UUID>>() }

        //Since all property types have been authorized simply have to read the appropriate entity set
        //Gather up all the authorized property types.
        val etAuthorizedPropertyTypes = constraint.entitySetIds.map{ it to authorizedPropertyTypes[it]!!}.toMap()
//        constraint.
        val propertyTypes = etAuthorizedPropertyTypes.values
                .flatMap { it.values }
                .toSet()
                .map { it.id to it.type.fullQualifiedNameAsString }
                .toMap()
        TODO( "Finish implementing")
//        return selectEntitySetWithCurrentVersionOfPropertyTypes(
//                entityKeyIds,
//                propertyTypes,
//                listOf(),
//                etAuthorizedPropertyTypes.mapValues { it.value.keys },
//                filters,
//                setOf(MetadataOption.LINKING_ID),
//                false,
//                etAuthorizedPropertyTypes.associate { it.id to (it.datatype == EdmPrimitiveTypeKind.Binary) }


//        )
    }

    /*
     * In order to extract custom index, we need subgraph and list of properties.
     *
     * So anchor joined into each src/dst, then edges inner joined on dst/src
     *
     * So target dataset looks like
     *
     * es_a | es_b | es_c
     */

}