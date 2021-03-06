package com.github.fjdbc.sql;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.GregorianCalendar;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;

import com.github.fjdbc.sql.SqlBuilder.Placement;
import com.github.fjdbc.sql.SqlBuilder.SqlFragment;
import com.github.fjdbc.sql.SqlBuilder.SqlSelectClause;

/**
 * This class conforms to the POJO convention of maven surefire.
 * <p>
 * It writes SQL statements in the file StandardSqlTest-last.txt, then compares the content with a reference file
 * (StandardSqlTest-ref.txt).
 */
public class SqlBuilderTest {
	private static final File last = new File(
			SqlBuilderTest.class.getClassLoader().getResource("StandardSqlTest-last.txt").getFile());
	private static final File ref = new File(
			SqlBuilderTest.class.getClassLoader().getResource("StandardSqlTest-ref.txt").getFile());
	private final BufferedWriter writer;

	public SqlBuilderTest() throws IOException {
		writer = new BufferedWriter(new FileWriter(last));
	}

	public void writeSql() throws IOException {
		final SqlBuilder sql = new SqlBuilder(null, SqlDialect.STANDARD, true);
		// @formatter:off
		writeSql(sql
			.select("a", "b")
			.from("table1")
		);
		writeSql(sql
			.select("a", "b")
			.from("table1")
			.where(sql.condition("a").gte().value(1))
		);
		writeSql(sql
			.select("a", "b")
			.from("table1")
			.where(sql.condition("a").gt().value(1))
			.where(sql.condition("b").eq().value("toto"))
		);
		writeSql(sql
			.select("a", "b")
			.from("table1")
			.where(sql.or(
				sql.condition("a").lt().value(1),
				sql.condition("b").notEq().value("toto")
			))
		);
		writeSql(sql
			.select("a", "b")
			.from("table1")
			.where(sql.and(
				sql.condition("a").gt().value(new BigDecimal(BigInteger.valueOf(205), 2)),
				sql.condition("b").isNotNull()
			))
		);
		writeSql(sql
			.select("a", "b")
			.from("table1")
			.where(sql.condition("a").in(sql.select("a").from("table2")))
			.where(sql.condition("c").gt().all(sql.select("c").from("table3")))
			.where(sql.condition("d").lt().any(sql.select("d").from("table4")))
		);
		writeSql(sql
			.select("a", "b")
			.from("table1")
			.where(sql.condition("a").eq().subquery(sql.select("1").from("dual")))
		);
		writeSql(sql
			.select("count(*)")
			.from("table1")
			.having(sql.condition("count(*)").gte().value(2))
		);
		writeSql(sql
			.with("t").as(sql.select("a").from("table2"))
			.select("t.a, b")
			.from("table1")
			.innerJoin("t on table1.b = t.a")
		);
		writeSql(sql
			.deleteFrom("table1")
			.where(sql.condition("a").lte().raw("1+1"))
		);
		writeSql(sql
			.update("table1")
			.set("a").value(1)
		);
		writeSql(sql
			.update("table1")
			.set("a").value(1)
			.where(sql.condition("a").isNull())
		);
		writeSql(sql
			.mergeInto("table1")
			.on("a").value("2")
			.insertOrUpdate("b").value(3)
			.insert("c").value(new java.sql.Date(new GregorianCalendar(2010, 2, 3).getTimeInMillis()))
		);
		writeSql(sql.union(
			sql.select("1").from("dual"),
			sql.select("1").from("dual"))
		);
		writeSql(sql.minus(
			sql.select("1").from("dual"),
			sql.select("1").from("dual"))
		);
		writeSql(sql.except(
			sql.select("1").from("dual"),
			sql.select("1").from("dual"))
		);
		writeSql(sql
			.select("1")
			.from("dual")
			.where(sql.not(
				sql.condition("a").eq().value(1)
			))
		);
		writeSql(sql
			.select("1")
			.from("dual")
			.where("a").in_String(Collections.emptyList())
		);
		writeSql(sql
				.select("1")
				.raw(Placement.BEFORE_KEYWORD, SqlSelectClause.SELECT, "raw_before_select")
				.raw(Placement.AFTER_KEYWORD, SqlSelectClause.SELECT, "raw_after_select")
				.raw(Placement.AFTER_KEYWORD, SqlSelectClause.SELECT, "raw_after_select2")
				.raw(Placement.AFTER_EXPRESSION, SqlSelectClause.SELECT, "raw_after_select_expr")
				.raw(Placement.BEFORE_KEYWORD, SqlSelectClause.FROM, "raw_before_from")
				.from("dual")
				.raw(Placement.AFTER_KEYWORD, SqlSelectClause.FROM, "raw_after_from")
				.raw(Placement.AFTER_EXPRESSION, SqlSelectClause.FROM, "raw_after_from_expr")
				);
		//@formatter:on
	}

	public void writeSql(SqlFragment sqlFragment) throws IOException {
		writer.write(sqlFragment.getSql());
		writer.write("\n\n");
	}

	@Test
	public void testAll() throws IOException {
		writeSql();
		writer.flush();
		assert FileUtils.contentEquals(last, ref);
	}

	@After
	public void tearDown() throws IOException {
		writer.close();
	}
}
