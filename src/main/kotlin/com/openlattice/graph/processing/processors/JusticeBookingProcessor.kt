package com.openlattice.graph.processing.processors

import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.datastore.services.EdmManager
import com.openlattice.graph.processing.util.NONE
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.temporal.ChronoUnit
import java.util.*

@Component
class JusticeBookingProcessor(edmManager: EdmManager, entityDataService:PostgresEntityDataQueryService):
        BaseDurationProcessor(edmManager, entityDataService) {
    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getSql(): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    private val handledEntityType = "justice.booking"

    companion object {
        private val logger = LoggerFactory.getLogger(JusticeBookingProcessor.javaClass)
    }

    override fun processAssociations(newEntities: Map<UUID, Any?>) {
    }

    override fun getLogger(): Logger {
        return logger
    }

    override fun getPropertyTypeForStart(): String {
        return "date.booking"
    }

    override fun getPropertyTypeForEnd(): String {
        return "publicsafety.ReleaseDate"
    }

    override fun getPropertyTypeForDuration(): String {
        return "criminaljustice.timeserveddays"
    }

    override fun getTimeUnit(): ChronoUnit {
        return ChronoUnit.DAYS
    }

    override fun getTransformationType(): String {
        return NONE
    }

    fun handledEntityTypes(): Set<UUID> {
        return setOf(getEntityTypeId(handledEntityType))
    }

}