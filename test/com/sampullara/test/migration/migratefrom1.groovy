/**
 * Groovy script to do a migration
 *
 * User: sam
 * Date: Sep 23, 2007
 * Time: 1:51:21 PM
 */
import com.sampullara.db.Migrate
import java.util.logging.Level

Migrate.logger.log(Level.INFO, "Running the migration class rather than the script directly");
Migrate.sqlScriptMigrator(connection, "com/sampullara/test/migration/migrate1.sql");
 