package com.dataloom.apps.processors;

import com.dataloom.apps.App;
import com.dataloom.edm.requests.MetadataUpdate;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Map;
import java.util.UUID;

public class UpdateAppMetadataProcessor extends AbstractRhizomeEntryProcessor<UUID, App, Object> {
    private static final long serialVersionUID = 1677128107090126118L;
    @SuppressFBWarnings( value = "SE_BAD_FIELD", justification = "Custom Stream Serializer is implemented" )

    private final MetadataUpdate update;

    public UpdateAppMetadataProcessor( MetadataUpdate update ) {
        this.update = update;
    }

    @Override public Object process( Map.Entry<UUID, App> entry ) {
        App app = entry.getValue();
        if ( app != null ) {
            if ( update.getTitle().isPresent() ) {
                app.setTitle( update.getTitle().get() );
            }
            if ( update.getDescription().isPresent() ) {
                app.setDescription( update.getDescription().get() );
            }
            if ( update.getName().isPresent() ) {
                app.setName( update.getName().get() );
            }
            entry.setValue( app );
        }
        return null;
    }

    public MetadataUpdate getUpdate() {
        return update;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        UpdateAppMetadataProcessor that = (UpdateAppMetadataProcessor) o;

        return update.equals( that.update );
    }

    @Override public int hashCode() {
        return update.hashCode();
    }
}
