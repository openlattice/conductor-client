package com.openlattice.assembler

import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.postgres.mapstores.OrganizationAssemblyMapstore
import com.openlattice.tasks.HazelcastInitializationTask
import com.openlattice.tasks.PostConstructInitializerTaskDependencies
import com.openlattice.tasks.Task
import org.slf4j.LoggerFactory
import java.util.*

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class OrganizationAssembliesInitializerTask : HazelcastInitializationTask<Assembler> {

    companion object {
        @JvmStatic
        private val logger = LoggerFactory.getLogger(OrganizationAssembliesInitializerTask::class.java)

        private val THIS_ORG_PREDICATE: Predicate<AclKey, SecurableObjectType> =
                Predicates.equal( "this", SecurableObjectType.Organization)
        private val ORG_ASSEMBLY_INIT_PREDICATE : Predicate<UUID, OrganizationAssembly> =
                Predicates.equal(OrganizationAssemblyMapstore.INITIALIZED_INDEX, true)
    }

    override fun getInitialDelay(): Long {
        return 0
    }

    override fun initialize(dependencies: Assembler) {
        val currentOrganizations = dependencies.securableObjectTypes.keySet( THIS_ORG_PREDICATE )
                .map { it.first() }
                .toSet()

        val initializedOrganizations = dependencies.assemblies.keySet(ORG_ASSEMBLY_INIT_PREDICATE)

        val organizationsNeedingInitialized: Set<UUID> = currentOrganizations - initializedOrganizations

        organizationsNeedingInitialized.forEach { organizationId ->
            val organizationPrincipal = dependencies.principals[AclKey(organizationId)]
            if (organizationPrincipal == null) {
                logger.error( "Unable to initialize organization with id {} because principal not found",
                        organizationId)
            } else {
                logger.info("Initializing database for organization {}", organizationId)
                dependencies.createOrganization(organizationId)
            }
        }
    }

    override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
        return setOf(
                EntitySetViewsInitializerTask::class.java,
                PostConstructInitializerTaskDependencies.PostConstructInitializerTask::class.java
        )
    }

    override fun getName(): String {
        return Task.ORGANIZATION_ASSEMBLIES_INITIALIZER.name
    }

    override fun getDependenciesClass(): Class<out Assembler> {
        return Assembler::class.java
    }
}
