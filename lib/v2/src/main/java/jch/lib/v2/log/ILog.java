package jch.lib.v2.log;

import jch.lib.v2.log.impl.QLog;
import jch.lib.v2.log.impl.QLogEntry;
import java.io.IOException;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import lombok.Getter;

/***
 * ILog -
 * <p>
 * Intended to be an adaptable interface with QLog being the main implementation target.
 * @author Chad Harrison
 * @param <T> Intended to be flexible to adapt to other log types
 */
public interface ILog<T> {

    @Getter
    enum Level {
        OFF(0),
        FATAL(100),
        ERROR(200),
        WARN(300),
        INFO(400),
        DEBUG(500),
        TRACE(600),
        ALL(700);

        private final int level;
        Level(int i) {this.level = i;}
    }

    void log(Object msg);
    void log(Object msg, Level level);
    void debug(Object msg);
    void error(Object msg);
    void fatal(Object msg);
    void info(Object msg);
    void warn(Object msg);
    void trace(Object msg);
    void catching(Object msg);
    void throwing(Object msg);
    ArrayList<ILog<T>> getAllLoggers();

    //TODO: decide how to handle
    //public void traceEntry();
    //public void traceExit();


    /********* QLog Implementation Methods *********/

    static String toString(QLogEntry entry) {
        final int PAD1 = 5;
        final int PAD2 = 23;
        StringBuilder output = new StringBuilder();

        output.append("[").append(entry.getLevel().toString()).append("]");
        output.append(" ".repeat(PAD1 - entry.getLevel().toString().length()));

        if(entry.isPrintTimeDate()) {
            output.append(" ").append(entry.getDate().toString());
            output.append(" ".repeat(PAD2 - entry.getDate().toString().length()));
        }

        if(!entry.getLogger().isBlank()) output.append(" ").append(entry.getLogger());
        if(!entry.getEntryPrefix().isBlank()) output.append(" ").append(entry.getEntryPrefix());
        if(!output.isEmpty()) output.append(": ");

        output.append(entry.getMessage());
        return output.toString();
    }

    static QLogEntry toQLogEntry(QLog qlog, Object msg, Level level) {
        return new QLogEntry(
                qlog.getId(),
                qlog.getLoggerName(),
                level,
                qlog.getCallerName(),
                objToString(msg),       //objToString handles exception to String
                qlog.isPrintTimeDate(),
                qlog.isToConsole(),
                qlog.isToFile(),
                qlog.isToUi(),
                qlog.isToDb()
        );
    }

    static QLogEntry toQLogEntry(QLog qlog, Object msg) {
        return toQLogEntry(qlog, msg, qlog.getDefaultLevelEntry());
    }

    static void dispatch(QLog qlog, QLogEntry entry) {
        if(!qlog.isInitialized()) qlog.setInitialized(true);
        if(entry.getLevel() != Level.OFF) {
            Level level = entry.getLevel();
            if(entry.isToConsole() && level.ordinal() <= qlog.getConsoleLevel().ordinal()) {
                if(qlog.isPrintTrivial())
                    System.out.println(entry.getMessage());
                else
                    System.out.println(entry.toString());
            }

            if(entry.isToFile() && level.ordinal() <= qlog.getFileLevel().ordinal()) {
                printToFile(qlog, entry);
            }

            if(entry.isToDb() && level.ordinal() <= qlog.getDbLevel().ordinal()) {
                System.out.println("To DB: " + entry.toString());
            }

            if(entry.isToUi() && level.ordinal() <= qlog.getUiLevel().ordinal()) {
                System.out.println("To UI: " + entry.toString());
            }
        }
    }

    static void printToFile(QLog qlog, QLogEntry entry) {
        try {
            if(qlog.getFullFilePath().contentEquals("")) {
                qlog.setFullFilePath(ILog.buildFullFilePath(qlog));
                Files.createDirectory(Paths.get(qlog.getFilePath()));
            }
            if(qlog.isPrintTrivial()) {
                Files.writeString(Paths.get(entry.getFullFilePath()), entry.getMessage(), StandardOpenOption.APPEND);
            }
            else {
                Files.writeString(Paths.get(entry.getFullFilePath()), entry.toString(), StandardOpenOption.APPEND);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String buildFullFilePath(QLog qlog) {
        String timeStamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new java.util.Date());
        return qlog.getFilePath() + qlog.getLoggerName().trim() +
                "_" + timeStamp + "." + qlog.getFileExtension().trim();
    }

    private static String objToString(Object msg) {
        String output = "";

        if(msg != null) {
            output = msg.toString();
            if(msg instanceof Exception)
                output += " " + Arrays.toString(((Exception) msg).getStackTrace());
        }

        return output;
    }
}
