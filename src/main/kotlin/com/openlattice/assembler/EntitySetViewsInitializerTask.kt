package com.openlattice.assembler

import com.openlattice.organizations.tasks.OrganizationsInitializationTask
import com.openlattice.tasks.HazelcastInitializationTask
import com.openlattice.tasks.Task

/**
 * This class is responsible for refreshing all entity set views at startup.
 */
class EntitySetViewsInitializerTask : HazelcastInitializationTask<Assembler> {
    override fun getInitialDelay(): Long {
        return 0
    }

    override fun initialize(dependencies: Assembler) {
//            dependencies.entitySets.keys.forEach(dependencies::createOrUpdateProductionViewOfEntitySet)
    }

    override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
        return setOf(OrganizationsInitializationTask::class.java)
    }

    override fun getName(): String {
        return Task.ENTITY_VIEWS_INITIALIZER.name
    }

    override fun getDependenciesClass(): Class<out Assembler> {
        return Assembler::class.java
    }
}