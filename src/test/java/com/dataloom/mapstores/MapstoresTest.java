package com.dataloom.mapstores;

import java.util.Collection;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.HzAuthzTest;
import com.dataloom.edm.internal.AbstractSecurableObject;
import com.kryptnostic.rhizome.mapstores.TestableSelfRegisteringMapStore;


public class MapstoresTest extends HzAuthzTest {
    private static final Logger                                      logger = LoggerFactory
            .getLogger( MapstoresTest.class );
    @SuppressWarnings( "rawtypes" )
    private static final Collection<TestableSelfRegisteringMapStore> mapstores;
    static {
        mapstores = testServer.getContext().getBeansOfType( TestableSelfRegisteringMapStore.class ).values();
    }

    @Test
    public void testMapstore() {
        mapstores.stream().forEach( MapstoresTest::test );
    }

    @SuppressWarnings( { "rawtypes", "unchecked" } )
    private static void test( TestableSelfRegisteringMapStore ms ) {
        Object expected = ms.generateTestValue();
        Object key = ms.generateTestKey();
        if ( AbstractSecurableObject.class.isAssignableFrom( expected.getClass() )
                && UUID.class.equals( key.getClass() ) ) {
            key = ( (AbstractSecurableObject) expected ).getId();
        }
        Object actual = null;
        try {
            ms.store( key, expected );
            actual = ms.load( key );
            if( !expected.equals( actual ) ) {
                logger.error( "Incorrect r/w to mapstore {} for key {}. expected({}) != actual({})", ms.getMapName(), key, expected, actual );
            }
        } catch ( Exception e ) {
            logger.error( "Unable to r/w to mapstore {} value: ({},{})", ms.getMapName(), key, expected, e );
            throw e;
        }
        Assert.assertEquals( expected, actual );
    }
}
