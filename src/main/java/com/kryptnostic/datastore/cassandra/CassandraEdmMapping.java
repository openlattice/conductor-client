package com.kryptnostic.datastore.cassandra;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.core.edm.primitivetype.EdmPrimitiveTypeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.TypeCodec;

public class CassandraEdmMapping {
    private static final Logger          logger = LoggerFactory
            .getLogger( CassandraEdmMapping.class );
    
    public static String getCassandraTypeName( EdmPrimitiveTypeKind edmPrimitveTypeKind ) {
        return getCassandraType( edmPrimitveTypeKind ).getName().name();
    }

    // TODO: Consider replacing with EnumMap?
    public static DataType getCassandraType( EdmPrimitiveTypeKind edmPrimitveTypeKind ) {
        switch ( edmPrimitveTypeKind ) {
            case Binary:
                return DataType.blob();
            case Boolean:
                return DataType.cboolean();
            case Byte:
                // This will require special processing :-/
                return DataType.blob();
            case Date:
                return DataType.date();
            case DateTimeOffset:
                return DataType.timestamp();
            case Decimal:
                return DataType.decimal();
            case Double:
                return DataType.cdouble();
            case Duration:
                // There be dragons. Parsing format for durations looks terrible. Currently just storing string and
                // hoping for the best.
                return DataType.text();
            case Guid:
                return DataType.uuid();
            case Int16:
                return DataType.smallint();
            case Int32:
                return DataType.cint();
            case Int64:
                return DataType.bigint();
            case String:
                return DataType.text();
            case SByte:
                return DataType.tinyint();
            case Single:
                return DataType.cfloat();
            case TimeOfDay:
                return DataType.time();

            default:
                return DataType.blob();
        }
    }

    public static Object getJavaTypeFromPrimitiveTypeKind( String literal, EdmPrimitiveTypeKind type )
            throws EdmPrimitiveTypeException {
        String value = EdmPrimitiveTypeFactory.getInstance( type ).fromUriLiteral( literal );
        switch ( type ) {
            case String:
                return value;
            case Guid:
                return UUID.fromString( value );
            case Int64:
                return Long.parseLong( value );
            default:
                return value;
        }
    }
    
    
    /**
     * Given CQL Data Type, convert (Jackson-deserialized) Java object to Java object of correct type.
     * Correspondence between CQL Data type and Java class follows here:
     * https://docs.datastax.com/en/cql/3.3/cql/cql_reference/cql_data_types_c.html
     * http://docs.datastax.com/en/drivers/java/3.1/com/datastax/driver/core/TypeCodec.html
     * also See Jackson's UntypedObjectDeserializer for expected input of Java object, resulted from Jackson's raw data-binding.
     * @param input
     * @param dt Cassandra DataType
     * @return
     */
    public static Object recoverJavaTypeFromCqlDataType( Object input, DataType dt){
        try{
            switch( dt.getName() ){
                //Jackson would already deserialize input to java.lang.Number
                case BIGINT:
                case COUNTER:
                    return ( (Number) input ).longValue();
                case INT:
                    return ((Number) input).intValue();
                case SMALLINT:
                    return ( (Number) input ).shortValue();
                case TINYINT:
                    return ( (Number) input ).byteValue();
                //Jackson would already deserialize input to java.lang.Double, unless USE_BIG_DECIMAL_FOR_FLOATS is enabled.
                case DECIMAL:
                    return BigDecimal.valueOf( (Double) input );
                case FLOAT:
                    return ( (Double) input).floatValue();
                case TIMESTAMP:
                    return TypeCodec.timestamp().parse( input.toString() );
                case DATE:
                    return TypeCodec.date().parse( input.toString() );
                case TIME:
                    return TypeCodec.time().parse( "'" + input.toString() + "'" );
                case UUID:
                case TIMEUUID:
                    return UUID.fromString( input.toString() );
                case INET:
                    return InetAddress.getByName( input.toString() );
                case BLOB:
                    return TypeCodec.blob().parse( input.toString() );
                //Jackson would already deserialize input to java.util.List, unless USE_JAVA_ARRAY_FOR_JSON_ARRAY is enabled.
                case SET:
                    return new HashSet<Object>( (List<?>) input ); 
                //Default cases (i.e. already converted by Jackson) include: BOOLEAN, DOUBLE, TEXT, VARCHAR, VARINT, LIST, MAP. 
                //Untreated cases include: CUSTOM (returns LinkedHashMap), TUPLE (return String)
                default: return input;
            }
        } catch ( Exception e ){
            logger.error( "Error writing data: Wrong data format." ,  e );            
        }
        return null;
    }
}
