package com.github.fjdbc.sql;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.github.fjdbc.connection.SingleConnectionProvider;
import com.github.fjdbc.query.SingleRowExtractor;
import com.github.fjdbc.sql.SqlBuilder.SqlInsertBuilder;
import com.github.fjdbc.sql.SqlBuilder.SqlSelectBuilder;

/**
 * The examples used in README.MD
 */
public class ReadmeExamples {
	private static final SqlBuilder sql = new SqlBuilder(null);

	public static void main(String[] args) throws Exception {
		// -------------------------------------------------------------------------------------------------------------
		// Setup
		final Connection cnx = DriverManager.getConnection("/jdbc/url");
		final SingleConnectionProvider provider = new SingleConnectionProvider(cnx);
		final SqlBuilder sql = new SqlBuilder(provider);

		// @formatter:off
		// Select data from the database
		{
			final SqlSelectBuilder builder = sql.select("ename").from("emp").where("empno").eq().value(1);
			final SingleRowExtractor<String> extractor = rs -> rs.getString("ename");
			builder.toQuery(extractor).toList(); // returns ["KING"]
		}

		// DELETE examples
		{ 
			sql.deleteFrom("emp")
			.where("job").notEq().value("SALESMAN");
		}	

		// UPDATE examples
		{ 
			sql.update("emp").set("salary").raw("salary * 2");
		}

		// INSERT examples
		// Simple INSERT
		{ 
			final SqlInsertBuilder builder = sql.insertInto("emp");
			builder.set("ename").value("KING");
			builder.set("job").value("PRESIDENT");
			builder.set("hiredate").value(new java.sql.Date(new Date().getTime()));
		}

		// INSERT with subquery
		{ 
			final SqlInsertBuilder builder = sql.insertInto("emp2");
			builder.subquery("ename", "job").select("ename", "job").from("emp");
		}

		// SELECT examples
		// SELECT with aggregate function
		{ 
			sql.select("job", "count(*)")
			.from("emp")
			.groupBy("job");
		}

		// Compound statement: UNION
		{ 
			sql.union(
			    sql.select("job").from("emp"),
			    sql.select("job").from("emp2")
			);
		}

		// SELECT with AND/OR operators
		{ 
			final SqlSelectBuilder builder = sql.select("*");
			builder.from("emp");
			builder.where(sql.or(
			    sql.condition("job").eq().value("SALESMAN"),
			    sql.condition("name").eq().value("KING")
			));
			// Multiple WHERE clauses are joined with AND
			builder.where("hiredate").lt().raw("SYSDATE - 1");
		}

		// SELECT with JOIN clause
		{ 
			sql.select("e.ename", "d.deptname")
			.from("emp e")
			.innerJoin("dept d on e.deptno = d.deptno");
		}
		

		// Select with subquery
		{ 
			sql.select("*")
			.from("emp")
			.where("deptno").eq().subquery(
			    sql.select("deptno")
			    .from("dept")
			    .where("deptname").eq().value("SALES")
			);
		}

		// Select with IN clause
		{ 
			final Collection<String> names = Arrays.asList("KING", "ALLEN");
			sql.select("*")
			.from("emp")
			.where("ename").in_String(names);
		}
		// @formatter:on

		// Batch statement examples
		// Batch statement with input data coming from a Collection
		{
			final List<String> values = Arrays.asList("SALES", "ACCOUNTING");
			final List<SqlInsertBuilder> inserts = values.stream().map(ReadmeExamples::createInsertStatement)
					.collect(Collectors.toList());
			sql.batchStatement(inserts).executeAndCommit();
		}

		// Batch statement with input data coming from a Stream
		{
			final Stream<SqlInsertBuilder> stream = Files.lines(Paths.get("c:/my/file.txt"))
					.map(ReadmeExamples::createInsertStatement);
			// the empty string is used to generate the SQL, but anything else would
			// have been fine since we are dealing with a prepared statement.
			final String sqlString = createInsertStatement("").getSql();
			sql.batchStatement(sqlString, stream).toStatement().executeAndCommit();
		}
	}

	private static SqlInsertBuilder createInsertStatement(String deptname) {
		final SqlInsertBuilder builder = sql.insertInto("dept");
		builder.values().set("deptname").value(deptname);
		return builder;
	}
}
