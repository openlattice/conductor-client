package com.dataloom.graph.core;

import java.util.UUID;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.core.impl.Label;
import com.google.common.collect.SetMultimap;

/**
 * Service handling CRUD operations of {@link Label}s.
 * @author Ho Chung Siu
 *
 */
public interface LoomGraphLabelManager {
    public Label addLabel( EntityKey key, Label.Type type );
    
    public Label getLabel( UUID id );
    
    public void updateLabel( UUID id, SetMultimap<UUID, Object> newValues );

    public void deleteLabel( UUID id );
}
