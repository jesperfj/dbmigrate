package com.sampullara.db;

import java.util.List;

import junit.framework.TestCase;

public class SqlStatementParserTest extends TestCase {
	private StringBuilder sb;
	private SqlStatementParser underTest;
	
	@Override
	protected void setUp() throws Exception {
		this.sb = new StringBuilder();
		this.underTest = new SqlStatementParser(sb);
	}
	
	public void testShouldParseSingleTerminatedStatement() {
		// given
		sb.append("SELECT blah FROM whatever;");
		
		// when
		List<String> statements = underTest.pullStatements();
		
		// then
		assertEquals(1, statements.size());
		assertEquals("SELECT blah FROM whatever", statements.get(0));

		assertEquals("", sb.toString());
	}
	
	public void testShouldParseMultipleStatements() {
		// given
		sb.append("SELECT blah FROM whatever;");
		sb.append("SELECT foo FROM bar;");
		sb.append("SELECT baz FROM quux;");
		
		// when
		List<String> statements = underTest.pullStatements();
		
		// then
		assertEquals(3, statements.size());
		assertEquals("SELECT blah FROM whatever", statements.get(0));
		assertEquals("SELECT foo FROM bar", statements.get(1));
		assertEquals("SELECT baz FROM quux", statements.get(2));
		
		assertEquals("", sb.toString());
	}
	
	public void testShouldLeaveIncompleteStatement() {
		// given
		sb.append("SELECT blah FROM whatever;");
		sb.append("SELECT foo FRO");
		
		// when
		List<String> statements = underTest.pullStatements();
		
		// then
		assertEquals(1, statements.size());
		assertEquals("SELECT blah FROM whatever", statements.get(0));
		
		assertEquals("SELECT foo FRO", sb.toString());
	}
	
	public void testShouldIgnoreSemiColonInSingleQuotes() {
		// given
		sb.append("CREATE FUNCTION baz(int) RETURNS text AS 'something;';");
		
		// when
		List<String> statements = underTest.pullStatements();
		
		// then
		assertEquals(1, statements.size());
		assertEquals("CREATE FUNCTION baz(int) RETURNS text AS 'something;'",
					 statements.get(0));
		
		assertEquals("", sb.toString());
	}
	
	public void testShouldNotTerminateSingleQuotesForSlashEscapedQuoteCharacter() {
		// given
		sb.append("sql sql sql 'string string\\' string;' sql sql;");
		
		// when
		List<String> statements = underTest.pullStatements();
		
		// then
		assertEquals(1, statements.size());
		assertEquals("sql sql sql 'string string\\' string;' sql sql",
					 statements.get(0));
		
		assertEquals("", sb.toString());
	}
	
	public void testShouldNotTerminateSingleQuotesForQuoteEscapedQuoteCharacter() {
		// given
		sb.append("sql sql sql 'string string'' string;' sql sql;");
		
		// when
		List<String> statements = underTest.pullStatements();
		
		// then
		assertEquals(1, statements.size());
		assertEquals("sql sql sql 'string string'' string;' sql sql",
					 statements.get(0));
		
		assertEquals("", sb.toString());
	}
}
