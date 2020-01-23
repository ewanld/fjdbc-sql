package com.github.fjdbc.sql;

/**
 * Dialects are a way to tweak the SQL generation in case of non-standard behavior. It alse provide a way to work
 * around JDBC driver inconsistencies.
 */
public enum SqlDialect {
	/**
	 * Use this when no other dialect applies. Try to provide a behavior as standard as possible.
	 */
	STANDARD,
	/**
	 * Oracle database
	 */
	ORACLE
}