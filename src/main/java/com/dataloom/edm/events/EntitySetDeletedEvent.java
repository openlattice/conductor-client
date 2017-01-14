package com.dataloom.edm.events;

import java.util.UUID;

public class EntitySetDeletedEvent {
	
	private UUID entitySetId;
	
	public EntitySetDeletedEvent( UUID entitySetId ) {
		this.entitySetId = entitySetId;
	}
	
	public UUID getEntitySetId() {
		return entitySetId;
	}

}
