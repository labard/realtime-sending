package ru.at_consulting.dmp.ignite;

import java.io.InputStream;
import java.util.Properties;

class APProperties {
    private static final Properties PROPERTIES = new Properties();

    static {
        try (final InputStream stream = APProperties.class.getClassLoader().getResourceAsStream("default.properties")) {
            PROPERTIES.load(stream);
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    private APProperties() {}

    static String get(String name) {
        return PROPERTIES.getProperty(name);
    }
}
