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

pluginHome = new File("./plugins").listFiles().find {
    it.name.startsWith('dbmigrate-')}
includeTargets << new File("${pluginHome}/scripts/Migrate.groovy")

task('default': "Migrates the current database to the latest") {
    depends(createMigration)
}

task('createMigration': "Create the next migration in the series") {
    profile("make sure the database is up to date") {
        migrate();
    }

    profile("create the migration") {
        def database = migrate.getDatabaseName(migrate.connection);
        File dir = new File("${basedir}/grails-app/migrations/${database}")
        dir.mkdirs();
        int current = migrate.getDBVersion();
        File scriptFile = new File(dir, "migratefrom${current}.sql")
        scriptFile << "# Migration from version ${current}\n"
        if (current == 0) {
            scriptFile << "CREATE TABLE db_version (version integer NOT NULL);\n"
            scriptFile << "INSERT INTO db_version VALUES (1);\n"
        }
        System.out.println("Created new migration: " + scriptFile)
    }

}
