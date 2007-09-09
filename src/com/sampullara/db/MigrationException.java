package com.sampullara.db;

/**
 * Created by IntelliJ IDEA.
 * User: sam
 * Date: Sep 8, 2007
 * Time: 1:07:59 PM
 * To change this template use File | Settings | File Templates.
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
