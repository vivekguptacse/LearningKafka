package com.sample.vivek.kafka.learning.bootstrap;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import com.sample.vivek.kafka.learning.exception.PropertyReadException;
import com.sample.vivek.kafka.learning.util.LoadProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author : Vivek Kumar Gupta
 * @since : 20/07/20
 */
public class BootstrapModule extends AbstractModule {

    private static final Logger logger = LoggerFactory.getLogger(BootstrapModule.class);

    @Override
    protected void configure() {
        // Bind the properties

        try {
            Names.bindProperties(binder(), LoadProperties.loadPropertiesFile());
        } catch (PropertyReadException e) {
            logger.error("Error while reading configuration file.");
        }
    }
}
