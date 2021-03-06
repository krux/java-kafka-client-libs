package com.krux.kafka.helpers;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertiesUtils {

    private static final Logger LOG = LoggerFactory.getLogger( PropertiesUtils.class );

    public static Properties createPropertiesFromOptionSpec( OptionSet options ) {
        Properties props = new Properties();
        Map optionsMap = options.asMap();

        for ( Object o : optionsMap.keySet() ) {
            OptionSpec spec = (OptionSpec) o;
            Collection keys = spec.options();
            for ( Object o1 : keys ) {
                String key = String.valueOf( o1 );
                List values = options.valuesOf( key );
                for ( Object obj : values ) {
                	LOG.info( key + ": " + String.valueOf( obj ) );
                	props.put( key, String.valueOf( obj ) );
                }
            }
        }
        return props;
    }

}
