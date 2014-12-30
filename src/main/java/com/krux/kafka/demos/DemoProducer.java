package com.krux.kafka.demos;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.krux.kafka.producer.KafkaProducer;
import com.krux.stdlib.KruxStdLib;

/** A simple producer that will push messages to a passed-in topic and then exit */
public class DemoProducer {
    
    private static final Logger LOG = LoggerFactory.getLogger( DemoProducer.class );

    public static void main( String[] args ) {

        OptionParser parser = KafkaProducer.getStandardOptionParser();
        
        OptionSpec<Integer> numOfMessagesToSend = parser
                .accepts(
                        "num-of-messages-to-send",
                        "Total number of messages to send.")
                .withRequiredArg().ofType(Integer.class);

        // give parser to KruxStdLib so it can add our params to the reserved
        // list
        KruxStdLib.setOptionParser(parser);
        OptionSet options = KruxStdLib.initialize(args);
        
        //make sure we have what we need
        if ( !options.has( "topic" ) || !options.has(  "metadata.broker.list" ) || !options.has(  "num-of-messages-to-send" )) {
            LOG.error( "--topic, --metadata.broker.list and --num-of-messages-to-send are required cl params. Rerun with -h for more info." );
            System.exit( -1 );
        }

        // parse the configured port -> topic mappings, put in global hashmap
        Map<OptionSpec<?>, List<?>> optionMap = options.asMap();
        String topicName = (String)options.valueOf(  "topic"  );

        KafkaProducer producer = new KafkaProducer( options, topicName );

        Random r = new Random();
        int j = Math.abs( r.nextInt() );
        String runId = Integer.toHexString( j );
        int numMessages = options.valueOf((  numOfMessagesToSend ));
        
        LOG.info( "Sending " +  numMessages + " messages." );
        int count = 0;
        try {
            String host = InetAddress.getLocalHost().getHostName();
            for (int i = 0; i < numMessages; i++ ) {
                producer.send( runId + ": " + host );
                count++;
            }
            LOG.info( "Sent " +  count + " messages." );
        } catch ( Exception e ) {
            LOG.error( "Can't send messages", e );
        }
        
        //by default, krux producer is async.  Must force quit to kill the processing thread.
        System.exit( 0 );
    }

}
