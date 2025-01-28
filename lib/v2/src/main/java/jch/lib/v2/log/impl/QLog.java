package jch.lib.v2.log.impl;

import jch.lib.v2.log.ILog;
import java.util.List;
import java.util.ArrayList;
import jakarta.persistence.*;
import lombok.*;
import lombok.experimental.Accessors;
import com.fasterxml.jackson.annotation.JsonProperty;


/***
 *
 * @author Chad Harrison
 */
@Accessors(chain = true)
@Data @Builder
@NoArgsConstructor
@AllArgsConstructor
public class QLog implements ILog<QLog> {

    @Id @GeneratedValue(strategy = GenerationType.UUID) @Column(name = "entry_id")
    @JsonProperty("loggerId") private String id;

    @JsonProperty("entries")
    @Builder.Default private ArrayList<QLogEntry> entries = new ArrayList<>();

    @Builder.Default @JsonProperty("defaultLevelEntry") private Level defaultLevelEntry = Level.INFO;
    @Builder.Default @JsonProperty("defaultPrefix") private String defaultPrefix = "QLog";
    @Builder.Default @JsonProperty("filePath") private String filePath = System.getProperty("user.dir");
    @Builder.Default @JsonProperty("fileExtension") private String fileExtension = "txt";
    @Builder.Default @JsonProperty("logger") private String loggerName = "default_logger";
    @Builder.Default @JsonProperty("printTrivial")  private boolean printTrivial = false;
    @Builder.Default @JsonProperty("printTimeDate")  private boolean printTimeDate = true;
    @Builder.Default @JsonProperty("toConsole") private boolean toConsole = true;
    @Builder.Default @JsonProperty("toFile") private boolean toFile = false;
    @Builder.Default @JsonProperty("toUi") private boolean toUi = true;
    @Builder.Default @JsonProperty("toDb") private boolean toDb = false;
    @Builder.Default @JsonProperty("consoleLevel") private Level consoleLevel = Level.INFO;
    @Builder.Default @JsonProperty("fileLevel") private Level fileLevel = Level.DEBUG;
    @Builder.Default @JsonProperty("uiLevel") private Level uiLevel = Level.WARN;
    @Builder.Default @JsonProperty("dbLevel") private Level dbLevel = Level.INFO;
    @Builder.Default @JsonProperty("fullFilePath") private String fullFilePath = "";

    @Getter private boolean initialized = false;
    @Getter private List<QLog> allLoggers;

    public String getCallerName() {
        String output = "";
        for(StackTraceElement t : Thread.currentThread().getStackTrace()) {
            if(!t.getMethodName().contentEquals("log") &&
                    !t.getMethodName().contentEquals("dispatch") &&
                    !t.getMethodName().contentEquals("getStackTrace") &&
                    !t.getMethodName().contentEquals("getCallerName")) {
                output = t.getMethodName(); break;
            }
        }
        return output;
    }

    public void addEntry(QLogEntry entry) {
        this.entries.add(entry);
    }

    @Override
    public void log(Object msg) {
        this.addEntry(ILog.toQLogEntry(this, msg));
    }

    @Override
    public void log(Object msg, Level level) {
        this.addEntry(ILog.toQLogEntry(this, msg, level));
    }

    @Override
    public void debug(Object msg) {
        this.addEntry(ILog.toQLogEntry(this, msg, Level.DEBUG));
    }

    @Override
    public void error(Object msg) {
        this.addEntry(ILog.toQLogEntry(this, msg, Level.ERROR));
    }

    @Override
    public void fatal(Object msg) {
        this.addEntry(ILog.toQLogEntry(this, msg, Level.FATAL));
    }

    @Override
    public void info(Object msg) {
        this.addEntry(ILog.toQLogEntry(this, msg, Level.INFO));
    }

    @Override
    public void warn(Object msg) {
        this.addEntry(ILog.toQLogEntry(this, msg, Level.WARN));
    }

    @Override
    public void trace(Object msg) {
        this.addEntry(ILog.toQLogEntry(this, msg, Level.TRACE));
    }

    @Override
    public void catching(Object msg) {
        this.addEntry(ILog.toQLogEntry(this, msg, Level.ERROR));
    }

    @Override
    public void throwing(Object msg) {
        this.addEntry(ILog.toQLogEntry(this, msg, Level.FATAL));
    }
}
