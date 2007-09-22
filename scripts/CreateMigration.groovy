/*
* Copyright 2004-2005 the original author or authors.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

/**
 * Gant script that handles the migration of databases
 *
 * @author Sam Pullara
 *
 * @since 1.0
 */

grailsAppName = ""

Ant.property(environment: "env")
grailsHome = Ant.antProject.properties."env.GRAILS_HOME"

task('default': "Migrates the current database to the latest") {
    depends(createMigration)
}

task('createMigration': "Create the next migration in the series") {
    profile("make sure the database is up to date") {
        migrate();
    }

    profile("create the migration") {
        def database = migrate.connection.metaData.databaseProductName.toLowerCase() + "/";
        File dir = new File("${basedir}/grails-app/migrations")
        dir.mkdirs();
        int current = migrate.getDBVersion();
        File scriptFile = new File(dir, "${database}migratefrom${current}.sql")
        scriptFile << "# Migration from version ${current}\n"
        System.out.println("Created new migration: " + scriptFile)
    }

}

task ('migrate' : "Migrates the current database to the latest") {
    profile("compiling config") {
        compile()
    }

    profile("creating config object") {
        ClassLoader contextLoader = Thread.currentThread().getContextClassLoader()
        classLoader = new URLClassLoader([classesDir.toURL()] as URL[], contextLoader)
        def configSlurper = new ConfigSlurper(grailsEnv)
        def configFile = new File("${basedir}/grails-app/conf/Config.groovy")
        if (configFile.exists()) {
            try {

                config = configSlurper.parse(classLoader.loadClass("Config"))
                config.setConfigFile(configFile.toURL())

                ConfigurationHolder.setConfig(config)
            }
            catch (Exception e) {
                e.printStackTrace()

                event("StatusFinal", ["Failed to compile configuration file ${configFile}: ${e.message}"])
                exit(1)
            }

        }
        def dataSourceFile = new File("${basedir}/grails-app/conf/DataSource.groovy")
        if (dataSourceFile.exists()) {
            try {
                def dataSourceConfig = configSlurper.parse(classLoader.loadClass("DataSource"))
                config.merge(dataSourceConfig)
                ConfigurationHolder.setConfig(config)
            }
            catch (Exception e) {
                e.printStackTrace()

                event("StatusFinal", ["Failed to compile data source file $dataSourceFile: ${e.message}"])
                exit(1)
            }
        }
        classLoader = contextLoader;
    }

    profile("automigrate the current database") {
        Properties p = config.dataSource.toProperties();
        p.driver = p.driverClassName;
        p.user = p.username;
        if (p.password == null) {
            p.password = ""
        }
        p.packageName = "grails-app/migrations"
        p.auto = "true";
        migrate = new Migrate(p)
        try {
            System.out.println("Migrating ${grailsEnv} database");
            if (migrate.migrate()) {
                System.out.println("Database migrated");
            } else {
                System.out.println("Database up-to-date");
            }
        }
        catch (Exception e) {
            e.printStackTrace()
            event("StatusFinal", ["Failed to migrate database ${grailsEnv}"])
            exit(1)
        }
    }
}
