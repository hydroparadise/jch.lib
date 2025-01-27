package jch.lib.v2.log.impl;

import jakarta.persistence.*;
import jch.lib.v2.log.ILog;
import jch.lib.v2.log.ILog.Level;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;
import java.time.*;


/***
 * Immutable QLog Entry
 * @author Chad Harrison
 */

//loggerId, loggerName, level, prefix, message, printTimeDate, toConsole, toFile, toUi, toDb
@RequiredArgsConstructor
@Getter
@Entity
public class QLogEntry {

    @JsonProperty("loggerId") @Column(name = "logger_id")
    private final String loggerId;

    @Id @GeneratedValue(strategy = GenerationType.IDENTITY) @Column(name = "entry_id")
    @JsonProperty("entryId") private Integer id;  //non-final to exclude in constructor from @RequiredArgsConstructor

    @JsonProperty("date") @Column(name = "date")
    private final ZonedDateTime date = ZonedDateTime.now(ZoneId.of("UTC"));

    @JsonProperty("logger") @Column(name = "logger")
    private final String logger;

    @JsonProperty("level") @Column(name = "level")
    private final Level level;

    @JsonProperty("prefix") @Column(name = "prefix")
    private final String entryPrefix;

    @JsonProperty("message") @Column(name = "message")
    private final String message;

    @JsonProperty("printTimeDate") @Column(name = "print_date")
    private final boolean printTimeDate;

    @JsonProperty("toConsole") @Column(name = "console")
    private final boolean toConsole;

    @JsonProperty("toFile") @Column(name = "file")
    private final boolean toFile;

    @JsonProperty("toUi") @Column(name = "ui")
    private final boolean toUi;

    @JsonProperty("toDb") //redundant to report to db that db is being used?
    private final boolean toDb;

    @JsonProperty("fullFilePath") @Column(name = "file_path")
    private final String fullFilePath = "";

    public String toString() {return ILog.toString(this);}
}
