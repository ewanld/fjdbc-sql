package com.github.fjdbc.sql;
import java.sql.Connection;
import java.sql.SQLException;

import com.github.fjdbc.connection.SingleConnectionProvider;
import com.github.fjdbc.query.SingleRowExtractor;
import com.github.fjdbc.sql.StandardSql.SqlSelectBuilder;
import com.github.fjdbc.sql.StandardSql.SqlUpdateBuilder;

/**
 * The examples used in README.MD
 */
public class ReadmeExamples {
	public static void main(String[] args) throws SQLException {
		// -------------------------------------------------------------------------------------------------------------
		// Setup
		final Connection connection = null;
		final StandardSql sql = new StandardSql(new SingleConnectionProvider(connection), true);

		// Simple SELECT
		{
			final SqlSelectBuilder builder = sql.select("name");
			builder.from("user");
			builder.where("id").eq().value(14);
			System.out.println(builder.getSql());
			final SingleRowExtractor<String> extractor = rs -> rs.getString("name");
			// final List<String> names = builder.toQuery(extractor).toList();
		}

		// SELECT DISTINCT
		{
			final SqlSelectBuilder builder = sql.selectDistinct("name");
			builder.from("user");
			System.out.println(builder.getSql());
		}

		// Expression in SELECT clause
		{
			final SqlSelectBuilder builder = sql.select("'Mr' || name");
			builder.from("user");
			System.out.println(builder.getSql());
		}

		// Where clauses are joined with AND by default
		{
			final SqlSelectBuilder builder = sql.select("name");
			builder.from("user");
			builder.where("age").gte().value(18);
			builder.where("age").lte().value(25);
			System.out.println(builder.getSql());
		}

		// SELECT with OR clause
		{
			final SqlSelectBuilder builder = sql.select("name");
			builder.from("user");
			builder.where("visible").eq().value(1);
			builder.where(sql.or(sql.condition("age").eq().value(18), sql.condition("age").eq().value(25)));
			System.out.println(builder.getSql());
		}

		// Sub query in WHERE clause
		{
			final SqlSelectBuilder builder = sql.select("name");
			builder.from("user");
			builder.where("id").in(sql.select("id").from("visible_user"));
			System.out.println(builder.getSql());
		}

		// Simple UPDATE
		{
			final SqlUpdateBuilder builder = sql.update("user");
			builder.set("name").value("John");
			builder.where("id").eq().value(14);
			System.out.println(builder.getSql());
			// builder.toStatement().executeAndCommit();
		}

	}
}
