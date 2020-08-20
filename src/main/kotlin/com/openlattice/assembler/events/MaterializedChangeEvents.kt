package com.openlattice.assembler.events

import com.openlattice.authorization.Principal
import com.openlattice.authorization.securable.SecurableObjectType
import java.util.*

data class MaterializedEntitySetEdmChangeEvent(val entitySetId: UUID)

data class MaterializedEntitySetDataChangeEvent(val entitySetId: UUID)

data class MaterializePermissionChangeEvent(
        val organizationPrincipal: Principal,
        val entitySetIds: Set<UUID>,
        val objectType: SecurableObjectType
)
