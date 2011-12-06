package com.inmobi.databus.datamovement;

import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import  java.util.Properties;
import java.util.*;
import java.io.*;

/**
 * Created by IntelliJ IDEA.
 * User: inderbir.singh
 * Date: 04/12/11
 * Time: 1:52 PM
 * To change this template use File | Settings | File Templates.
 */
public class PropertiesReader {
    Logger logger = Logger.getLogger(PropertiesReader.class);
    private static final boolean THROW_ON_LOAD_FAILURE = true;
        private static final boolean LOAD_AS_RESOURCE_BUNDLE = false;
        private static final String SUFFIX = ".properties";


  /**
     * Looks up a resource named 'name' in the classpath. The resource must map
     * to a file with .properties extention. The name is assumed to be absolute
     * and can use either "/" or "." for package segment separation with an
     * optional leading "/" and optional ".properties" suffix. Thus, the
     * following names refer to the same resource:
     * <pre>
     * some.pkg.Resource
     * some.pkg.Resource.properties
     * some/pkg/Resource
     * some/pkg/Resource.properties
     * /some/pkg/Resource
     * /some/pkg/Resource.properties
     * </pre>
     *
     * @param name classpath resource name [may not be null]
     * @param loader classloader through which to load the resource [null
     * is equivalent to the application loader]
     *
     * @return resource converted to java.util.Properties [may be null if the
     * resource was not found and THROW_ON_LOAD_FAILURE is false]
     * @throws IllegalArgumentException if the resource was not found and
     * THROW_ON_LOAD_FAILURE is true
     */
    public static Properties loadProperties (String name, ClassLoader loader)
    {
        if (name == null)
            throw new IllegalArgumentException ("null input: name");

        if (name.startsWith ("/"))
            name = name.substring (1);

        if (name.endsWith (SUFFIX))
            name = name.substring (0, name.length () - SUFFIX.length ());

        Properties result = null;

        InputStream in = null;
        try
        {
            if (loader == null) loader = ClassLoader.getSystemClassLoader ();

            if (LOAD_AS_RESOURCE_BUNDLE)
            {
                name = name.replace ('/', '.');
                // Throws MissingResourceException on lookup failures:
                final ResourceBundle rb = ResourceBundle.getBundle (name,
                    Locale.getDefault (), loader);

                result = new Properties ();
                for (Enumeration keys = rb.getKeys (); keys.hasMoreElements ();)
                {
                    final String key = (String) keys.nextElement ();
                    final String value = rb.getString (key);

                    result.put (key, value);
                }
            }
            else
            {
                name = name.replace ('.', '/');

                if (! name.endsWith (SUFFIX))
                    name = name.concat (SUFFIX);

                // Returns null on lookup failures:
                in = loader.getResourceAsStream (name);
                if (in != null)
                {
                    result = new Properties ();
                    result.load (in); // Can throw IOException
                }
            }
        }
        catch (Exception e)
        {
            result = null;
        }
        finally
        {
            if (in != null) try { in.close (); } catch (Throwable ignore) {}
        }

        if (THROW_ON_LOAD_FAILURE && (result == null))
        {
            throw new IllegalArgumentException ("could not load [" + name + "]"+
                " as " + (LOAD_AS_RESOURCE_BUNDLE
                ? "a resource bundle"
                : "a classloader resource"));
        }

        return result;
    }

    /**
     * A convenience overload of {@link #loadProperties(String, ClassLoader)}
     * that uses the current thread's context classloader.
     */
    public  Properties loadScribeProperties (final String name)
    {
        if (name == null) {
            return loadProperties ("scribe.properties",
                        Thread.currentThread ().getContextClassLoader ());

        }
        else {
            try {
                Properties properties = new Properties();
                 properties.load(new FileInputStream(name));
                return properties;
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                logger.debug(e);
            }
        }

        return null;
    }


    //test case
    public static void main(String[] args) {
        PropertiesReader propertiesReader = new PropertiesReader();
        //load from classpath
        Properties properties = propertiesReader.loadScribeProperties(null);

        System.out.println("com.inmobi.databus.scribe.logs.parentdir = " + properties.getProperty("com.inmobi.databus.scribe.logs.parentdir"));
    }
   }
