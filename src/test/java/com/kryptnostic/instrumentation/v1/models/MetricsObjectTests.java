//package com.kryptnostic.instrumentation.v1.models;
//
//import java.io.IOException;
//import java.security.InvalidAlgorithmParameterException;
//import java.security.InvalidKeyException;
//import java.security.NoSuchAlgorithmException;
//import java.security.SignatureException;
//import java.security.spec.InvalidKeySpecException;
//import java.security.spec.InvalidParameterSpecException;
//
//import javax.crypto.BadPaddingException;
//import javax.crypto.IllegalBlockSizeException;
//import javax.crypto.NoSuchPaddingException;
//
//import org.joda.time.DateTime;
//import org.junit.Assert;
//import org.junit.Ignore;
//import org.junit.Test;
//
//import com.fasterxml.jackson.annotation.JsonIgnore;
//import com.google.common.hash.HashCode;
//import com.google.common.hash.HashFunction;
//import com.kryptnostic.kodex.v1.crypto.ciphers.BlockCiphertext;
//import com.kryptnostic.kodex.v1.crypto.keys.Kodex.SealedKodexException;
//import com.kryptnostic.kodex.v1.exceptions.types.SecurityConfigurationException;
//import com.kryptnostic.kodex.v1.serialization.crypto.Encryptable;
//import com.kryptnostic.storage.v1.models.EncryptableBlock;
//import com.kryptnostic.storage.v1.models.KryptnosticObject;
//import com.kryptnostic.storage.v1.models.ObjectMetadata;
//import com.kryptnostic.utils.SecurityConfigurationTestUtils;
//
//@SuppressWarnings( "javadoc" )
//public class MetricsObjectTests extends SecurityConfigurationTestUtils {
//
//    @Test
//    public void testEquals() throws SecurityConfigurationException, IOException, ClassNotFoundException,
//            InvalidKeyException, NoSuchAlgorithmException, InvalidAlgorithmParameterException,
//            IllegalBlockSizeException, BadPaddingException, NoSuchPaddingException, InvalidKeySpecException,
//            InvalidParameterSpecException, SealedKodexException, SignatureException, Exception {
//
//  
//        public MetricsMetadata(
//                String id,
//                int version,
//                String logMessage,
//                String type ) {
//            this( id, version, logMessage, type, DateTime.now() );
//        }
//    	
//    	
//        MetricsMetadata
//
//        Assert.assertEquals( d1, d1 );
//        Assert.assertEquals( d1, d2 );
//
//        KryptnosticObject d3 = new KryptnosticObject( new ObjectMetadata( "test2" ), "cool document" ).encrypt( loader );
//        Assert.assertNotEquals( d1, d3 );
//
//        KryptnosticObject d4 = new KryptnosticObject( new ObjectMetadata( "test" ), "cool document cool" )
//                .encrypt( loader );
//        Assert.assertEquals( d1, d4 );
//        Assert.assertNotEquals( d1, d3 );
//    }
//
//}
