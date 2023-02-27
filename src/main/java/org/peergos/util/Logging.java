package org.peergos.util;


import java.io.*;
import java.nio.file.Path;
import java.util.logging.*;

public class Logging {
    private static final Logger LOG = Logger.getGlobal();
    private  static final String NULL_FORMAT =  "NULL_FORMAT";
    private static final Logger NULL_LOG = Logger.getLogger(NULL_FORMAT);

    private static boolean isInitialised = false;
    public static Logger LOG() {
        return LOG;
    }
    private static Logger nullLog() {
        return NULL_LOG;
    }

    public static synchronized void init() {
        Path logPath = Path.of("nabu.%g.log");
        logPath.toFile().getParentFile().mkdirs();
        int logLimit = 1024 * 1024;
        int logCount = 10;
        boolean logAppend = true;
        boolean logToConsole = false;
        boolean logToFile = true;
        boolean printLogLocation =  true;

        NULL_LOG.setParent(LOG());

        init(logPath, logLimit, logCount, logAppend, logToConsole, logToFile, printLogLocation);
    }

    public static synchronized void init(Path logPath,
                                         int logLimit,
                                         int logCount,
                                         boolean logAppend,
                                         boolean logToConsole,
                                         boolean logToFile,
                                         boolean printLocation) {

        if (isInitialised)
            return;

        try {
            // also logging to stdout?
            if (! logToConsole)
                LOG().setUseParentHandlers(false);
            if (! logToFile)
                return;

            String logPathS = logPath.toString();
            FileHandler fileHandler = new FileHandler(logPathS, logLimit, logCount, logAppend);
            fileHandler.setFormatter(new WithNullFormatter());

            // tell console where we're logging to
            if (printLocation && logToFile)
                LOG().info("Logging to "+ logPathS.replace("%g", "0"));
            nullLog().setParent(LOG());

            Thread.setDefaultUncaughtExceptionHandler((thread, throwable) -> {
                long id = thread.getId();
                String name = thread.getName();
                String msg = "Uncaught Exception in thread " + id + ":" + name;
                LOG().log(Level.SEVERE, msg, throwable);
            });

            LOG().addHandler(fileHandler);
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe.getMessage(), ioe);
        } finally {
            isInitialised = true;
        }
    }

    private static final Formatter SIMPLE_FORMATTER = new SimpleFormatter();
    private static class WithNullFormatter  extends Formatter {

        /**
         * If the logger-name is NULL_FORMAT just post the message, otherwise use SimpleFormatter.format.
         * @param logRecord
         * @return
         */
        @Override
        public String format(LogRecord logRecord) {
            boolean noFormatting = NULL_FORMAT.equals(logRecord.getLoggerName());

            if (noFormatting)
                return logRecord.getMessage() + "\n";
            return SIMPLE_FORMATTER.format(logRecord);
        }
    }

}
