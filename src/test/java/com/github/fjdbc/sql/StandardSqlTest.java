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

import com.github.fjdbc.sql.StandardSql;
import com.github.fjdbc.sql.StandardSql.SqlFragment;

/**
 * This class conforms to the POJO convention of maven surefire.
 * <p>
 * It writes SQL statements in the file StandardSqlTest-last.txt, then compares the content with a reference file
 * (StandardSqlTest-ref.txt).
 */
public class StandardSqlTest {
	private static final File last = new File(
			StandardSqlTest.class.getClassLoader().getResource("StandardSqlTest-last.txt").getFile());
	private static final File ref = new File(
			StandardSqlTest.class.getClassLoader().getResource("StandardSqlTest-ref.txt").getFile());
	private final BufferedWriter writer;

	public StandardSqlTest() throws IOException {
		writer = new BufferedWriter(new FileWriter(last));
	}

	public void writeSql() throws IOException {
		final StandardSql sql = new StandardSql(null, true);
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
		//@formatter:on
	}

	public void writeSql(SqlFragment sqlFragment) throws IOException {
		writer.write(sqlFragment.getSql());
		writer.write("\n\n");
	}

	public void testAll() throws IOException {
		writeSql();
		writer.flush();
		assert FileUtils.contentEquals(last, ref);
	}

	public void tearDown() throws IOException {
		writer.close();
	}
}
