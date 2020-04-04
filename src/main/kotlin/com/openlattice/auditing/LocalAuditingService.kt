package com.openlattice.auditing

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.ArrayListMultimap
import com.google.common.collect.ImmutableMap
import com.openlattice.data.DataEdge
import com.openlattice.data.DataGraphManager
import com.openlattice.data.EntityDataKey
import java.util.*

class LocalAuditingService(
        private val dataGraphService: DataGraphManager,
        private val ares: AuditRecordEntitySetsManager,
        private val mapper: ObjectMapper
) : AuditingManager {

    fun recordEvent(event: AuditableEvent): Int {
        return recordEvents(listOf(event))
    }

    override fun recordEvents(events: List<AuditableEvent>): Int {

        val auditingConfiguration = ares.auditingTypes

        if (!auditingConfiguration.enabled()) {
            return 0
        }
        if (!auditingConfiguration.isAuditingInitialized()) {
            return 0
        }

        return events
                .groupBy { ares.getActiveAuditEntitySetIds(it.aclKey, it.eventType) }
                .filter { (auditEntitySetConfiguration, _) ->
                    auditEntitySetConfiguration.auditRecordEntitySet != null
                }
                .map { (auditEntitySetConfiguration, auditableEvents) ->
                    val auditEntitySet = auditEntitySetConfiguration.auditRecordEntitySet
                    val (entityKeyIds, _) = dataGraphService.createEntities(
                            auditEntitySet!!,
                            mapAuditableEventsToEntities(auditableEvents),
                            auditingConfiguration.propertyTypes
                    )

                    if (auditEntitySetConfiguration.auditEdgeEntitySet != null) {
                        val auditEdgeEntitySet = auditEntitySetConfiguration.auditEdgeEntitySet

                        val lm = ArrayListMultimap.create<UUID, DataEdge>()
                        entityKeyIds.asSequence().zip(auditableEvents.asSequence())
                                .filter { it.second.entities.isPresent }
                                .forEach { (auditEntityKeyId, ae) ->
                                    val aeEntitySetId = ae.aclKey[0]
                                    val aeEntityKeyIds = ae.entities.get()
                                    aeEntityKeyIds.forEach { id ->
                                        lm.put(
                                                auditEdgeEntitySet,
                                                DataEdge(
                                                        EntityDataKey(aeEntitySetId, id),
                                                        EntityDataKey(auditEntitySet, auditEntityKeyId),
                                                        ImmutableMap.of()
                                                )
                                        )
                                        return@forEach
                                    }
                                }
                        dataGraphService
                                .createAssociations(lm, ImmutableMap.of(auditEdgeEntitySet, emptyMap()))

                    }
                    entityKeyIds.size
                }.sum()
    }

    private fun mapAuditableEventsToEntities(events: List<AuditableEvent>): List<MutableMap<UUID, MutableSet<Any>>> {
        val auditingConfiguration = ares.auditingTypes
        return events.map { event ->
            val eventEntity = mutableMapOf<UUID, MutableSet<Any>>()

            eventEntity[auditingConfiguration.getPropertyTypeId(
                    AuditProperty.ACL_KEY
            )] = mutableSetOf<Any>(event.aclKey.index)

            event.entities.ifPresent {
                eventEntity[auditingConfiguration.getPropertyTypeId(AuditProperty.ENTITIES)] = it as MutableSet<Any>
            }

            event.operationId.ifPresent {
                eventEntity[auditingConfiguration.getPropertyTypeId(AuditProperty.OPERATION_ID)] = mutableSetOf<Any>(it)
            }

            eventEntity[auditingConfiguration.getPropertyTypeId(AuditProperty.ID)] = mutableSetOf<Any>(
                    event.aclKey.last().toString()
            ) //ID of securable object
            eventEntity[auditingConfiguration.getPropertyTypeId(AuditProperty.PRINCIPAL)] = mutableSetOf<Any>(
                    event.principal.toString()
            )
            eventEntity[auditingConfiguration.getPropertyTypeId(AuditProperty.EVENT_TYPE)] = mutableSetOf<Any>(
                    event.eventType.name
            )
            eventEntity[auditingConfiguration.getPropertyTypeId(AuditProperty.DESCRIPTION)] = mutableSetOf<Any>(
                    event.description
            )
            eventEntity[auditingConfiguration.getPropertyTypeId(AuditProperty.DATA)] = mutableSetOf<Any>(
                    mapper.writeValueAsString(event.data)
            )
            eventEntity[auditingConfiguration.getPropertyTypeId(AuditProperty.TIMESTAMP)] = mutableSetOf<Any>(
                    event.timestamp
            )

            return@map eventEntity
        }
    }
}