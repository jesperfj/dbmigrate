import com.sampullara.db.Migrate
import javax.sql.DataSource

/**
* Migrate the database first thing
*
* User: sam
* Date: Sep 23, 2007
* Time: 11:37:06 AM
*/

class ApplicationBootStrap {
    // Injected datasource
    def DataSource dataSource

    def init = {
        def grailsEnv = System.getProperty("grails.env")
        Migrate.logger.info("Migrating database ${grailsEnv}");

        // Set the package name and automigration
        Properties p = new Properties();
        p.packageName = "grails-app.migrations"
        p.auto = "true";

        // Create the migration service
        Migrate migrate = new Migrate(p, dataSource);

        // Migrate the database
        if (migrate.migrate()) {
            Migrate.logger.info("Database ${grailsEnv} migrated");
        } else {
            Migrate.logger.info("Database ${grailsEnv} up-to-date");
        }
    }

    def destroy = {

    }
}
