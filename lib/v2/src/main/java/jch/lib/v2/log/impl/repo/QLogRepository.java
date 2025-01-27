package jch.lib.v2.log.impl.repo;

import jch.lib.v2.log.impl.*;


public interface QLogRepository {

    public static String sqlCreateTable() {
        return """
CREATE TABLE qlog (
   loggerId VARCHAR(255),
   entryId INT,
   date TIMESTAMP WITH TIME ZONE,
   level int,
   prefix VARCHAR(50),
   message LONGVARCHAR,
   print_date BOOLEAN,
   console BOOLEAN,
   file BOOLEAN,
   ui BOOLEAN,
   file_path VARCHAR(500)
)
                """;
    }
}
