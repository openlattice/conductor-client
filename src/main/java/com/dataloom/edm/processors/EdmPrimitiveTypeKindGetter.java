package com.dataloom.edm.processors;

import java.util.Map.Entry;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.edm.internal.PropertyType;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

public class EdmPrimitiveTypeKindGetter extends AbstractRhizomeEntryProcessor<UUID, PropertyType, EdmPrimitiveTypeKind> {
    private static final long            serialVersionUID      = 4485807443899509297L;
    private static final Logger          logger                = LoggerFactory
            .getLogger( EdmPrimitiveTypeKindGetter.class );
    public static EdmPrimitiveTypeKindGetter GETTER = new EdmPrimitiveTypeKindGetter();

    @Override
    public EdmPrimitiveTypeKind process( Entry<UUID, PropertyType> entry ) {
        PropertyType pt = entry.getValue();
        if ( pt == null ) {
            logger.error( "Unable to retireve primitive type for property: {}", entry.getKey() );
            return null;
        }
        return pt.getDatatype();
    }

}
