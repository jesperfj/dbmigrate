package com.sampullara.test.migration;

import com.sampullara.db.Migrate;
import com.sampullara.db.MigrationException;
import com.sampullara.db.Migrator;

import java.sql.Connection;
import java.util.logging.Level;

/**
 * Migration class
 * 
 * User: sam
 * Date: Sep 8, 2007
 * Time: 3:30:08 PM
 */
public class Migrate2 implements Migrator {
    public void migrate(Connection conn) throws MigrationException {
        Migrate.logger.log(Level.INFO, "Running the migration class rather than the script directly");
        Migrate.scriptMigrator(conn, "com/sampullara/test/migration/migrate2.sql");
    }
}
