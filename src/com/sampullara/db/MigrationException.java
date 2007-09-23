package com.sampullara.db;

/**
 * Generic migration exception
 * 
 * User: sam
 * Date: Sep 8, 2007
 * Time: 1:07:59 PM
 */
public class MigrationException extends Exception {
    public MigrationException() {
        super();    //To change body of overridden methods use File | Settings | File Templates.
    }

    public MigrationException(String s) {
        super(s);    //To change body of overridden methods use File | Settings | File Templates.
    }

    public MigrationException(String s, Throwable throwable) {
        super(s, throwable);    //To change body of overridden methods use File | Settings | File Templates.
    }

    public MigrationException(Throwable throwable) {
        super(throwable);    //To change body of overridden methods use File | Settings | File Templates.
    }
}
