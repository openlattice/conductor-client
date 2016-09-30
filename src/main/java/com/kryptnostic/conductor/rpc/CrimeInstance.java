package com.kryptnostic.conductor.rpc;

import java.io.IOException;
import java.io.Serializable;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

@JsonPropertyOrder( { "id", "cn", "dt", "crime" } )
public class CrimeInstance implements Serializable {
    private static final long serialVersionUID = 9105115065612223050L;
    
    public static class CrimeInstanceCsvReader {
        private static final transient CsvMapper mapper = new CsvMapper();
        private static final transient CsvSchema schema = mapper.schemaFor( CrimeInstance.class );
        private static final Logger              logger = LoggerFactory.getLogger( CrimeInstanceCsvReader.class );
        
        static {
            logger.info( "Schema: {}", schema.getColumnDesc() );
        }
        
        public static CrimeInstance getCrimeInstance( String row ) throws IOException {
            try {
                return mapper.reader( CrimeInstance.class ).with( schema ).readValue( row );
            } catch ( IOException e ) {
                logger.error( "Something went wrong parsing row: {}", row, e );
                throw e;
            }
        }
    }
    
    private static final DateTimeFormatter DATETIMEFORMATTER = DateTimeFormat.forPattern( "dd/MM/yyyy HH:mm:ss aa" );
    private final String id;
    private final String caseNumber;
    private final DateTime dateTime;
    private final String crime;
    
    @JsonCreator
    public CrimeInstance(
            @JsonProperty( "id" ) String id,
            @JsonProperty( "cn" ) String caseNumber,
            @JsonProperty( "dt" ) String dateTime,
            @JsonProperty( "crime" ) String crime) {
        this.id = id;
        this.caseNumber = caseNumber;
        this.dateTime = DATETIMEFORMATTER.parseDateTime( dateTime );
        this.crime = crime;
    }
    
    @JsonProperty( "id" )
    public String getId() {
        return id;
    }
    
    @JsonProperty( "cn" )
    public String getCaseNumber() {
        return caseNumber;
    }
    
    @JsonProperty( "dt" )
    public DateTime getDateTime() {
        return dateTime;
    }
    
    @JsonProperty( "crime" )
    public String getCrime() {
        return crime;
    }

    @Override
    public String toString() {
        return "Criminal [id=" + id + ", crime=" + crime + "]";
    }
}