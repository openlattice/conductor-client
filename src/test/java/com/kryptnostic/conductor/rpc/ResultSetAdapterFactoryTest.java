package com.kryptnostic.conductor.rpc;

import static org.junit.Assert.assertEquals;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.kryptnostic.rhizome.configuration.RhizomeConfiguration;
import com.kryptnostic.rhizome.configuration.cassandra.CassandraConfiguration;
import com.kryptnostic.rhizome.configuration.service.ConfigurationService;

public class ResultSetAdapterFactoryTest {
    static ResultSet                      rs;
    static Map<String, FullQualifiedName> map      = new HashMap<String, FullQualifiedName>();

    static Integer                        lengthColumn;
    static List<String>                   columnNameList;
    static List<String>                   typeList;
    static List<String>                   NameList;
    static List<FullQualifiedName>        FQNList;

    static Integer                        lengthRow;
    static ArrayList<List<Object>>        rowData;
    static Random                         rand     = new Random();
    static String                         keyspace = "test_result_set_conversion_" + rand.nextInt( 10_000 );

    @BeforeClass
    // Setup a table called "test_result_set_conversion". The columns have names from columnNameList with type specified
    // in typeList.
    public static void SetupCassandraDBforTesting() throws UnknownHostException {
        // initialize Columns
        columnNameList = Arrays.asList( "property_id", "property_score", "property_text" );
        // Hard-coded test; right now only accepts int and text. Adding new type requires modifying "Wrapper when type
        // is text" lines below.
        typeList = Arrays.asList( "int", "int", "text" );
        NameList = Arrays.asList( "id", "score", "text" );
        lengthColumn = NameList.size();

        FQNList = new ArrayList<FullQualifiedName>();
        for ( int i = 0; i < lengthColumn; i++ ) {
            FQNList.add( new FullQualifiedName( "test_result_set_conversion", NameList.get( i ) ) );
        }

        // Initialize data to be entered
        rowData = new ArrayList<List<Object>>();
        rowData.add( Arrays.asList( 1, 100, "foo" ) );
        rowData.add( Arrays.asList( 3, 57, "bar" ) );
        rowData.add( Arrays.asList( 123, 3, "foobar" ) );
        rowData.add( Arrays.asList( 24, 0, "lol" ) );
        rowData.add( Arrays.asList( 10, 10, "test" ) );
        lengthRow = rowData.size();
        RhizomeConfiguration rc = ConfigurationService.StaticLoader.loadConfiguration( RhizomeConfiguration.class );
        CassandraConfiguration cassandraConfiguration = rc.getCassandraConfiguration().get();

        // Create Cassandra session
        Cluster cluster = new Cluster.Builder()
                .withCompression( cassandraConfiguration.getCompression() )
                .withPoolingOptions( new PoolingOptions() )
                .withProtocolVersion( ProtocolVersion.V4 )
                .addContactPoints( cassandraConfiguration.getCassandraSeedNodes() )
                .build();
        Session session = cluster.connect();

        // Create keyspace and table for testing
        session.execute(
                "CREATE KEYSPACE IF NOT EXISTS " + keyspace
                        + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}" );
        session = cluster.connect( keyspace );

        String tableCreation = "CREATE TABLE IF NOT EXISTS " + keyspace + ".Test \n(";
        for ( int i = 0; i < lengthColumn; i++ ) {
            tableCreation += columnNameList.get( i ) + " " + typeList.get( i ) + ", ";
        }
        tableCreation += "PRIMARY KEY (" + columnNameList.get( 0 ) + ")"
                + ");";

        session.execute( tableCreation );

        // Insert into table
        String queryHeader = "INSERT INTO " + keyspace + ".Test ( ";
        for ( int i = 0; i < lengthColumn; i++ ) {
            queryHeader += columnNameList.get( i );
            if ( i != lengthColumn - 1 ) queryHeader += ",";
        }
        queryHeader += ") VALUES (";

        for ( int i = 0; i < lengthRow; i++ ) {
            String insertTable = new String( queryHeader );
            for ( int j = 0; j < lengthColumn; j++ ) {
                // Wrapper when type is text
                if ( typeList.get( j ) == "text" ) {
                    insertTable += "'";
                }

                insertTable += rowData.get( i ).get( j );

                // Wrapper when type is text
                if ( typeList.get( j ) == "text" ) {
                    insertTable += "'";
                }

                if ( j != lengthColumn - 1 ) insertTable += ", ";
            }
            insertTable += ");";
            session.execute( insertTable );
        }

        // Query results
        rs = session.execute( "SELECT * FROM " + keyspace + ".Test;" );

        // Declare TypeName to FQN map
        for ( int i = 0; i < lengthColumn; i++ ) {
            map.put( columnNameList.get( i ), FQNList.get( i ) );
        }

        // Close Cassandra session
        cluster.close();
    }

    @Test
    public void Test() {
        Function<Row, SetMultimap<FullQualifiedName, Object>> function = ResultSetAdapterFactory.toSetMultimap( map );
        Iterable<SetMultimap<FullQualifiedName, Object>> convertedData = Iterables.transform( rs, function );

        // Initialize set for convertedData
        Set<SetMultimap<FullQualifiedName, Object>> convertedDataSet = new HashSet<SetMultimap<FullQualifiedName, Object>>();
        Iterator<SetMultimap<FullQualifiedName, Object>> it = convertedData.iterator();
        while ( it.hasNext() ) {
            convertedDataSet.add( it.next() );
        }

        // Initialize answer
        Set<SetMultimap<FullQualifiedName, Object>> ans = new HashSet<SetMultimap<FullQualifiedName, Object>>();
        for ( int i = 0; i < lengthRow; i++ ) {
            SetMultimap<FullQualifiedName, Object> obj = HashMultimap.create();
            for ( int j = 0; j < lengthColumn; j++ ) {
                obj.put( FQNList.get( j ), rowData.get( i ).get( j ) );
            }
            ans.add( obj );
        }

        // Compare
        assertEquals( convertedDataSet, ans );
    }

    @AfterClass
    public static void RemoveTestingTable() {
        // Remove table created for this test after.
        Cluster cluster = Cluster.builder().addContactPoint( "localhost" ).build();
        Session session = cluster.connect();
        session.execute( "DROP KEYSPACE " + keyspace + ";" );

        // Close Cassandra session
        cluster.close();
    }
}
