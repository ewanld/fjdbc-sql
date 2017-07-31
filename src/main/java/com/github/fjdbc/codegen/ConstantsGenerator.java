package com.github.fjdbc.codegen;

import java.io.Writer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;
import java.util.TreeSet;

import com.github.fjdbc.query.SingleRowExtractor;

public class ConstantsGenerator extends AbstractWriter {
	private final Connection cnx;

	public ConstantsGenerator(Connection cnx, Writer writer) {
		super(writer);
		this.cnx = cnx;
	}

	private Set<String> listTables() throws SQLException {
		final DatabaseMetaData metaData = cnx.getMetaData();
		final ResultSet resultSet = metaData.getTables(null, null, null, null);
		final SingleRowExtractor<String> extractor = rs -> rs.getString("table_name");
		final Set<String> tables = new TreeSet<>();
		extractor.extractAll(resultSet, tables::add);
		resultSet.close();
		return tables;
	}

	private Set<String> listColumns() throws SQLException {
		final DatabaseMetaData metaData = cnx.getMetaData();
		final ResultSet resultSet = metaData.getColumns(null, null, "%", null);
		final SingleRowExtractor<String> extractor = rs -> rs.getString("column_name");
		final Set<String> tables = new TreeSet<>();
		extractor.extractAll(resultSet, tables::add);
		resultSet.close();
		return tables;
	}

	public void generate(String packageName) throws Exception {
		final Set<String> tables = listTables();
		final Set<String> columns = listColumns();
				
		writeln("package %s;", packageName);
		writeln();
		writeln("public class DbConstants {");
		writeln("	public static class Tables {");
		for(final String t : tables) {
			writeln("		public static final String %s = \"%s\";", t.toUpperCase(), t);
		}
		writeln("	}");
		writeln();
		writeln("	public static class Columns {");
		for (final String c : columns) {
			writeln("		public static final String %s = \"%s\";", c.toUpperCase(), c);
		}
		writeln("	}");
		writeln("");
		writeln("}");
	}
}
