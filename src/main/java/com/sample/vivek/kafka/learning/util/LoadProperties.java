package com.sample.vivek.kafka.learning.util;

import com.sample.vivek.kafka.learning.exception.PropertyReadException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Load the properties from the file.
 *
 * @author : Vivek Kumar Gupta
 * @since : 20/07/20
 */
public final class LoadProperties {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoadProperties.class);

    private static final String pFile = "config.properties";
    private static Properties props = null;

    public static String getProperty(String key) {
        try {
            if (props == null) {
                loadPropertiesFile();
            }
        } catch (PropertyReadException exception) {
            LOGGER.error(exception.getMessage(), exception);
            System.exit(1);
        }
        return props.getProperty(key, null);
    }

    public static Properties loadPropertiesFile()
            throws PropertyReadException {
        try {
            InputStream propsis = LoadProperties.class.getClassLoader().getResourceAsStream(pFile);
            props = new Properties();
            if (propsis != null) {
                props.load(propsis);
                propsis.close();
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Properties file has been Loaded ");
            }
            return props;
        } catch (Exception e) {
            throw new PropertyReadException("Property File config.properties failed to load ");
        }
    }

    /**
     * Method to read from file
     *
     * @param pFile file path
     * @return content string
     * @throws PropertyReadException when read fails
     */
    public static String loadFromFile(String pFile) throws PropertyReadException {
        try {
            Path path = Paths.get(LoadProperties.class.getClassLoader()
                    .getResource(requireNonNull(pFile, "File path cannot be null"))
                    .toURI());
            Stream<String> lines = Files.lines(path);
            String data = lines.collect(Collectors.joining("\n"));
            lines.close();
            return data;
        } catch (Exception exception) {
            throw new PropertyReadException(exception.getMessage(), exception);
        }
    }

}
