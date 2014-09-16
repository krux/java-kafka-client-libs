package com.krux.kafka.demos;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.krux.kafka.consumer.KafkaConsumer;
import com.krux.stdlib.KruxStdLib;

public class DemoConsumer {

    private static final Logger LOG = LoggerFactory.getLogger( KafkaConsumer.class );

    public static void main( String[] args ) {

        OptionParser parser = new OptionParser();

        // expose all kafka consumer config params to the cli   
        KafkaConsumer.addStandardOptionsToParser( parser );

        KruxStdLib.setOptionParser( parser );
        OptionSet options = KruxStdLib.initialize( args );

        // ensure required cl options are present
        if ( !options.has( "topic-threads" ) || !options.has( "group.id" ) || !options.has( "zookeeper.connect" ) ) {
            LOG.error( "'--topic-threads', '--group.id', and '--zookeeper.connect' and all required parameters. Exitting!" );
            System.exit( -1 );
        }
        
       //setup our message handler
        @SuppressWarnings( "unchecked" )
        final DemoHandler<Object> myHandler = new DemoHandler<Object>();
        
        KafkaConsumer runner = new KafkaConsumer( options, myHandler );
        
        //this starts non-daemon thread pools (i.e.: they'll prevent the jvm from closing as long as they're running) 
        runner.start();
        
        LOG.info( "All consumer threads running" );
        
        //Start a thread that will output the number of messages consumed 
        TimerTask r = new TimerTask() {
            @Override
            public void run() {
                LOG.info( "Messages consumed: " + myHandler.getCount()  );
            }
        };
        
        Timer t = new Timer();
        t.schedule( r, 3000, 3000 );

    }

}
