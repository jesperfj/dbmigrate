package com.sampullara.db;

import java.util.ArrayList;
import java.util.List;

/**
 * The parser needs to be far more robust to really work
 * in the general case. Unfortunately that means we might have to actually parse the DDL which
 * would not be the best since it varies from database to database.  At worse we need to handle
 * brackets, parens, quotes, etc.
 * 
 * Currently, it simply splits the input into statements by finding the
 * STATEMENT_TERMINATOR character (the semi-colon ';'). It ignores instances
 * of the terminator which appear inside a single-quoted section.  Single
 * quote characters can be included in such a section if they are escaped,
 * with either a preceding backslash (C-style) or single-quote (SQL-style)
 * character.
 * 
 * See the tests in SqlStatementParserTest for usage examples.
 */
public class SqlStatementParser {
	/** the semi-colon is understood to terminate a statement */
    private static final char STATEMENT_TERMINATOR = ';';
    /** the single-quote character */
    private static final char SINGLE_QUOTE = '\'';
    /** the backslash is used to escape the next character, i.e. prevent it
     *  being recognized as a SINGLE_QUOTE character */
	private static final char ESCAPE_CHAR = '\\';

	/** Stores the buffer containing the SQL to be parsed into statements */
	private final StringBuilder unparsedBuffer;
	
	/**
	 * Create a new SqlStatementParser, ready to parse statements from the
	 * front of the unparsedBuffer
	 * 
	 * @param unparsedBuffer contains the SQL to be parsed into statements
	 */
	public SqlStatementParser(StringBuilder unparsedBuffer) {
		this.unparsedBuffer = unparsedBuffer;
	}
	
	/**
	 * Removes all complete statements from the unparsedBuffer, and
	 * returns them (each without its terminating semi-colon).
	 * 
	 * Note: this method has side effects on the unparsedBuffer which
	 * was passed to the SqlStatementParser's constructor - if a statement
	 * is successfully parsed, it will be removed from the buffer.
	 * 
	 * @return a List of all the statements which could be parsed from
	 * 	the front of the unparsed buffer
	 */
	public List<String> pullStatements() {
		List<String> statements = new ArrayList<String>();
		
		int parsedIndex = -1;
		boolean inQuotes = false;
        for (int i = 0; i != this.unparsedBuffer.length(); ++i)
        {
        	char current = this.unparsedBuffer.charAt(i);
        	if (current == SINGLE_QUOTE) {
        		char previous = this.unparsedBuffer.charAt(i-1);
        		if (previous != ESCAPE_CHAR) {
        			inQuotes = !inQuotes;
        		}
        	}
        	else if (current == STATEMENT_TERMINATOR) {
        		if (!inQuotes) {
        			String sql = this.unparsedBuffer.substring(parsedIndex + 1, i);
        			statements.add(sql.trim());
        			parsedIndex = i;
        		}
        	}
        }
		
        this.unparsedBuffer.delete(0, parsedIndex + 1);
		
		return statements;
	}
}
