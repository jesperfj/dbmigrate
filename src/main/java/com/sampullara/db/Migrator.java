package com.sampullara.db;

import java.sql.Connection;

/**
 * Created by IntelliJ IDEA.
 * User: sam
 * Date: Sep 8, 2007
 * Time: 2:27:36 PM
 * To change this template use File | Settings | File Templates.
 */
public interface Migrator {
    public void migrate(Connection conn) throws MigrationException;
}
