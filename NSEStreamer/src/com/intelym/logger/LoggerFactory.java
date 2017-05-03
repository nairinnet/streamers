package com.intelym.logger;
import org.apache.log4j.PropertyConfigurator;
/**
 *
 * @author Hari Nair
 * @since Mar 2012
 * @version 1.0
 */
public class LoggerFactory {

    private static boolean configured = false;

    public static RiwaLogger getLogger(Class className) {
        if (configured) {
            return new RiwaLogger(className);
        }
        else {
            PropertyConfigurator.configure("src/conf/log4j.properties");
            configured = true;
            return new RiwaLogger(className);
        }
    }
    
    public static RiwaLogger getMessageLogger(Class className){
        if (configured) {
            return new RiwaLogger(className);
        }
        else {
            PropertyConfigurator.configure("messagelog4j.properties");
            configured = true;
            return new RiwaLogger(className);
        }
    }
}
