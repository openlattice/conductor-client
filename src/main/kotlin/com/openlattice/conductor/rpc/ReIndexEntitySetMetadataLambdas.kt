package com.openlattice.conductor.rpc

import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import java.io.Serializable
import java.util.*
import java.util.function.Function

data class ReIndexEntitySetMetadataLambdas(
        val entitySets: Map<EntitySet, Set<UUID>>,
        val propertyTypes: Map<UUID, PropertyType>
) : Function<ConductorElasticsearchApi, Boolean>, Serializable {

    override fun apply(api: ConductorElasticsearchApi): Boolean {
        return api.triggerEntitySetIndex(entitySets, propertyTypes)
    }
}