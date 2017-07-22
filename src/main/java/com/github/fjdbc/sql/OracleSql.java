package com.github.fjdbc.sql;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.github.fjdbc.ConnectionProvider;
import com.github.fjdbc.FjdbcException;
import com.github.fjdbc.PreparedStatementBinder;
import com.github.fjdbc.op.StatementOperation;
import com.github.fjdbc.query.Query;
import com.github.fjdbc.query.ResultSetExtractor;
import com.github.fjdbc.util.IntSequence;

/**
 * SQL generator using a fluent interface.
 */
public class OracleSql {
	/**
	 * Debug statements by printing the value of prepared values in a comment
	 * next to the '?' placeholder.
	 */
	private final boolean debug;
	private final ConnectionProvider cnxProvider;

	/**
	 * @param cnxProvider
	 *            The database connection provider.
	 */
	public OracleSql(ConnectionProvider cnxProvider) {
		this(cnxProvider, false);
	}

	/**
	 * @param cnxProvider
	 *            The database connection provider.
	 * @param debug
	 *            Debug statements by printing the value of prepared values in a
	 *            comment next to the '?' placeholder.
	 */
	public OracleSql(ConnectionProvider cnxProvider, boolean debug) {
		this.cnxProvider = cnxProvider;
		this.debug = debug;
	}

	/**
	 * Build a {@code SELECT} statement.
	 */
	public SqlSelectBuilder select() {
		final SqlSelectBuilder res = new SqlSelectBuilder();
		return res;
	}

	/**
	 * Build a {@code SELECT} statement.
	 */
	public SqlSelectBuilder select(String... _selects) {
		final SqlSelectBuilder res = new SqlSelectBuilder();
		res.select(Arrays.asList(_selects));
		return res;
	}

	/**
	 * Convenience method to build a {@code SELECT DISTINCT} statement.
	 */
	public SqlSelectBuilder selectDistinct(String... _selects) {
		final SqlSelectBuilder res = new SqlSelectBuilder().distinct();
		res.select(Arrays.asList(_selects));
		return res;
	}

	/**
	 * Build a {@code SELECT} statement that starts with a {@code WITH} clause
	 * (Oracle SQL only).
	 */
	public WithClauseBuilder with(String pseudoTableName) {
		return new SqlSelectBuilder().with(pseudoTableName);
	}

	/**
	 * Build an {@code UPDATE} statement.
	 */
	public SqlUpdateBuilder update(String tableName) {
		return new SqlUpdateBuilder(tableName);
	}

	/**
	 * Build a {@code DELETE} statement.
	 */
	public SqlDeleteBuilder deleteFrom(String fromClause) {
		return new SqlDeleteBuilder(fromClause);
	}

	/**
	 * Build an {@code INSERT} statement.
	 */
	public SqlInsertBuilder insertInto(String tableName) {
		return new SqlInsertBuilder(tableName);
	}

	/**
	 * Build a {@code MERGE} statement (Oracle SQL only).
	 */
	public SqlMergeBuilder mergeInto(String tableName) {
		return new SqlMergeBuilder(tableName);
	}

	/**
	 * Build an SQL condition, to use in {@code WHERE} clauses for instance.
	 * 
	 * @param lhs
	 *            The left-hand side of the condition.
	 */
	public ConditionBuilder<Condition> condition(String lhs) {
		final ConditionBuilder<Condition> res = new ConditionBuilder<>(lhs, null);
		res.setParent(res);
		return res;
	}

	/**
	 * Convert an arbitrary string to an SQL fragment.
	 * <p>
	 * The SQL fragment may then be used in any of the {@code raw} methods, or
	 * as an SQL {@code Condition}.
	 */

	public SqlRaw raw(String sql) {
		return new SqlRaw(sql);
	}

	/**
	 * Convert an arbitrary string and a prepared statement binder to an SQL
	 * fragment.
	 * <p>
	 * The SQL fragment may then be used in any of the {@code raw} methods, or
	 * as an SQL {@code Condition}.
	 */
	public SqlRaw raw(String sql, PreparedStatementBinder binder) {
		return new SqlRaw(sql, binder);
	}

	/**
	 * Build an {@code AND} condition.
	 * 
	 * @param conditions
	 *            The conditions to be joined with the {@code AND} operator.
	 */
	public Condition and(Condition... conditions) {
		return new CompositeCondition(Arrays.asList(conditions), LogicalOperator.AND);
	}

	/**
	 * Build an {@code OR} condition.
	 * 
	 * @param conditions
	 *            The conditions to be joined with the {@code OR} operator.
	 */
	public Condition or(Condition... conditions) {
		return new CompositeCondition(Arrays.asList(conditions), LogicalOperator.OR);
	}

	/**
	 * Build a {@code SELECT} statement that is the {@code UNION} of all
	 * specified {@SELECT} statements.
	 * 
	 * @param selects
	 *            The {@code SELECT} statements to join.
	 */
	public SqlSelectStatement union(SqlSelectBuilder... selects) {
		return new CompositeSqlSelectStatement(Arrays.asList(selects), "union\n");
	}

	/**
	 * Build a {@code SELECT} statement that is the {@code UNION ALL} of all
	 * specified {@SELECT} statements.
	 * 
	 * @param selects
	 *            The {@code SELECT} statements to join.
	 */
	public SqlSelectStatement unionAll(SqlSelectBuilder... selects) {
		return new CompositeSqlSelectStatement(Arrays.asList(selects), "union all\n");
	}

	/**
	 * Build a {@code SELECT} statement that is the intersection of all
	 * specified {@code SELECT} statements.
	 * 
	 * @param selects
	 *            The {@code SELECT} statements to join with the
	 *            {@code INTERSECT} operator.
	 */
	public SqlSelectStatement intersect(SqlSelectBuilder... selects) {
		return new CompositeSqlSelectStatement(Arrays.asList(selects), "intersect\n");
	}

	/**
	 * Build a {@code SELECT} statement that represents {@code a MINUS b}.
	 */
	public SqlSelectStatement minus(SqlSelectBuilder a, SqlSelectBuilder b) {
		return new CompositeSqlSelectStatement(Arrays.asList(a, b), "minus\n");
	}

	/**
	 * Build a batch statement. The batch statement is initially empty;
	 * statements must be added with the
	 * {@link BatchStatementBuilder#addStatement} method.
	 */
	public BatchStatementBuilder batchStatement() {
		return new BatchStatementBuilder();
	}

	/**
	 * Build a batch statement using the specified statements.
	 */
	public SqlStatement batchStatement(Collection<? extends SqlStatement> statements) {
		return new BatchStatementBuilder(statements);
	}

	/**
	 * Build a batch statement from an SQL string and a stream of prepared
	 * statement binders.
	 */
	public SqlStatement batchStatement(String sql, Stream<? extends PreparedStatementBinder> statements) {
		return new StreamBackedBatchStatement(sql, statements);
	}

	public enum RelationalOperator implements SqlFragment {
		EQ("="), NOT_EQ("<>"), GT(">"), GTE(">="), LT("<"), LTE("<="), LIKE("like"), IS("is"), IS_NOT("is not"), IN(
				"in");

		private final String value;

		RelationalOperator(String value) {
			this.value = value;
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			w.append(value);
		}
	}

	public static class InConditionBuilder implements Condition {
		private final String sql;
		private final PreparedStatementBinder binder;

		public <T> InConditionBuilder(SqlFragment lhs, Collection<? extends T> values, Class<T> type) {
			if (values == null) throw new IllegalArgumentException();

			if (values.isEmpty()) {
				sql = "1=0";
				binder = null;
			} else {
				final int maxItemsForInClause = 1000; // Oracle limit
				final ArrayList<String> sqlClauses = new ArrayList<String>(values.size() / 1000 + 1);
				final List<List<String>> subCollections =
						OracleSqlUtils.partition(Collections.nCopies(values.size(), "?"), maxItemsForInClause);
				for (final List<String> subCollection : subCollections) {
					sqlClauses.add(
							lhs.getSql() + " in (" + subCollection.stream().collect(Collectors.joining(", ")) + ")");
				}
				sql = sqlClauses.size() == 1
						? sqlClauses.get(0)
						: ("(" + sqlClauses.stream().collect(Collectors.joining(" or ")) + ")");

				binder = (ps, index) -> {
					for (final T value : values) {
						setAnyObject(ps, index.next(), value, type);
					}
				};
			}
		}

		@Override
		public void bind(PreparedStatement ps, IntSequence index) throws SQLException {
			if (binder != null) binder.bind(ps, index);
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			w.append(sql);
		}

	}

	public static class SqlRaw implements SqlFragment, Condition {
		private final String sql;
		/**
		 * May be null.
		 */
		private final PreparedStatementBinder binder;

		public SqlRaw(String sql) {
			this(sql, null);
		}

		public SqlRaw(String sql, PreparedStatementBinder binder) {
			assert sql != null;
			this.sql = sql;
			this.binder = binder;
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			w.append(sql);
		}

		@Override
		public void bind(PreparedStatement st, IntSequence index) throws SQLException {
			if (binder != null) binder.bind(st, index);
		}
	}

	public static class SqlStringBuilder {
		private final StringBuilder sb = new StringBuilder();
		private int indentLevel = 0;
		private boolean startLine = true;

		public SqlStringBuilder append(String sql) {
			if (startLine) sb.append(Collections.nCopies(indentLevel * 4, " ").stream().collect(Collectors.joining()));
			startLine = false;
			sb.append(sql);
			return this;
		}

		/**
		 * Convenience method
		 */
		public SqlStringBuilder appendln(SqlFragment sqlFragment) {
			sqlFragment.appendTo(this);
			appendln();
			return this;
		}

		/**
		 * Convenience method
		 */
		public SqlStringBuilder append(SqlFragment sqlFragment) {
			sqlFragment.appendTo(this);
			return this;
		}

		/**
		 * Convenience method
		 */
		public SqlStringBuilder appendln(String sql) {
			append(sql);
			appendln();
			return this;
		}

		public SqlStringBuilder appendln() {
			sb.append("\n");
			startLine = true;

			return this;
		}

		public void increaseIndent() {
			indentLevel++;
		}

		public void decreaseIndent() {
			indentLevel--;
		}

		public String getSql() {
			return sb.toString();
		}

		@Override
		public String toString() {
			return sb.toString();
		}
	}

	public class SqlLiteral<T> implements SqlFragment {
		private final T value;
		private final Class<T> type;
		private final String sql;

		public SqlLiteral(T value, Class<T> type) {
			this("?", value, type);
		}

		public SqlLiteral(String sql, T value, Class<T> type) {
			this.sql = sql;
			assert sql != null && sql.contains("?");
			this.value = value;
			this.type = type;
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			w.append(sql);
			if (debug) {
				w.append("  /* ");
				w.append(value == null ? "null" : OracleSqlUtils.escapeComment(value.toString()));
				w.append(" */");
			}
		}

		@Override
		public void bind(PreparedStatement st, IntSequence index) throws SQLException {
			setAnyObject(st, index.next(), value, type);
		}
	}

	public static class SqlFragmentWrapper implements SqlFragment {
		private final boolean increaseIndent;
		private final String before;
		private final SqlFragment wrapped;
		private final String after;

		public SqlFragmentWrapper(String before, SqlFragment wrapped, String after, boolean increaseIndent) {
			this.before = before;
			this.wrapped = wrapped;
			this.after = after;
			this.increaseIndent = increaseIndent;
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			w.append(before);
			if (increaseIndent) {
				w.appendln();
				w.increaseIndent();
			}
			w.append(wrapped);
			if (increaseIndent) w.decreaseIndent();
			w.append(after);
		}

		@Override
		public void bind(PreparedStatement ps, IntSequence index) throws SQLException {
			wrapped.bind(ps, index);
		}
	}

	/**
	 * @param
	 * 			<P>
	 *            the parent type
	 */
	public class ExpressionBuilder<P> implements SqlFragment {
		private SqlFragment wrapped;
		private final P parent;

		public ExpressionBuilder(P parent) {
			this.parent = parent;
		}

		// @formatter:off
		public P value(String value) { wrapped = new SqlLiteral<>(value, String.class); return parent; }
		public P value(BigDecimal value) { wrapped = new SqlLiteral<>(value, BigDecimal.class); return parent; }
		public P value(Boolean value) { wrapped = new SqlLiteral<>(value, Boolean.class); return parent; }
		public P value(Integer value) { wrapped = new SqlLiteral<>(value, Integer.class); return parent; }
		public P value(Long value) { wrapped = new SqlLiteral<>(value, Long.class); return parent; }
		public P value(Float value) { wrapped = new SqlLiteral<>(value, Float.class); return parent; }
		public P value(Double value) { wrapped = new SqlLiteral<>(value, Double.class); return parent; }
		public P value(byte[] value) { wrapped = new SqlLiteral<>(value, byte[].class); return parent; }
		public P value(java.sql.Date value) { wrapped = new SqlLiteral<>(value, java.sql.Date.class); return parent; }
		public P value(Time value) { wrapped = new SqlLiteral<>(value, Time.class); return parent; }
		public P value(Timestamp value) { wrapped = new SqlLiteral<>(value, Timestamp.class); return parent; }
		public P value(Clob value) { wrapped = new SqlLiteral<>(value, Clob.class); return parent; }
		public P value(Blob value) { wrapped = new SqlLiteral<>(value, Blob.class); return parent; }
		public P value(Array value) { wrapped = new SqlLiteral<>(value, Array.class); return parent; }
		public P value(Ref value) { wrapped = new SqlLiteral<>(value, Ref.class); return parent; }
		public P value(URL value) { wrapped = new SqlLiteral<>(value, URL.class); return parent; }
		
		public P value(String sql, String value) { wrapped = new SqlLiteral<>(sql, value, String.class); return parent; }
		public P value(String sql, BigDecimal value) { wrapped = new SqlLiteral<>(sql, value, BigDecimal.class); return parent; }
		public P value(String sql, Boolean value) { wrapped = new SqlLiteral<>(sql, value, Boolean.class); return parent; }
		public P value(String sql, Integer value) { wrapped = new SqlLiteral<>(sql, value, Integer.class); return parent; }
		public P value(String sql, Long value) { wrapped = new SqlLiteral<>(sql, value, Long.class); return parent; }
		public P value(String sql, Float value) { wrapped = new SqlLiteral<>(sql, value, Float.class); return parent; }
		public P value(String sql, Double value) { wrapped = new SqlLiteral<>(sql, value, Double.class); return parent; }
		public P value(String sql, byte[] value) { wrapped = new SqlLiteral<>(sql, value, byte[].class); return parent; }
		public P value(String sql, java.sql.Date value) { wrapped = new SqlLiteral<>(sql, value, java.sql.Date.class); return parent; }
		public P value(String sql, Time value) { wrapped = new SqlLiteral<>(sql, value, Time.class); return parent; }
		public P value(String sql, Timestamp value) { wrapped = new SqlLiteral<>(sql, value, Timestamp.class); return parent; }
		public P value(String sql, Clob value) { wrapped = new SqlLiteral<>(sql, value, Clob.class); return parent; }
		public P value(String sql, Blob value) { wrapped = new SqlLiteral<>(sql, value, Blob.class); return parent; }
		public P value(String sql, Array value) { wrapped = new SqlLiteral<>(sql, value, Array.class); return parent; }
		public P value(String sql, Ref value) { wrapped = new SqlLiteral<>(sql, value, Ref.class); return parent; }
		public P value(String sql, URL value) { wrapped = new SqlLiteral<>(sql, value, URL.class); return parent; }
		// @formatter:on

		public P raw(String _sql) {
			wrapped = new SqlRaw(_sql);
			return parent;
		}

		public P raw(String _sql, PreparedStatementBinder binder) {
			wrapped = new SqlRaw(_sql, binder);
			return parent;
		}

		/**
		 * @param subquery
		 *            A subquery that returns a single row.
		 */
		public P subquery(SqlSelectBuilder subquery) {
			wrapped = new SqlFragmentWrapper("(", subquery, ")", true);
			return parent;
		}

		public P all(SqlSelectBuilder subquery) {
			wrapped = new SqlFragmentWrapper("all (", subquery, ")", true);
			return parent;
		}

		public P any(SqlSelectBuilder subquery) {
			wrapped = new SqlFragmentWrapper("any (", subquery, ")", true);
			return parent;
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			wrapped.appendTo(w);
		}

		@Override
		public void bind(PreparedStatement ps, IntSequence index) throws SQLException {
			wrapped.bind(ps, index);
		}
	}

	static <T> void setAnyObject(PreparedStatement ps, int columnIndex, T o, Class<T> type) throws SQLException {
		if (o == null) {
			ps.setNull(columnIndex, java.sql.Types.OTHER);
		} else if (type.equals(String.class)) {
			ps.setString(columnIndex, (String) o);
		} else if (type.equals(BigDecimal.class)) {
			ps.setBigDecimal(columnIndex, (BigDecimal) o);
		} else if (type.equals(Boolean.class)) {
			ps.setBoolean(columnIndex, (Boolean) o);
		} else if (type.equals(Integer.class)) {
			ps.setInt(columnIndex, (Integer) o);
		} else if (type.equals(Long.class)) {
			ps.setLong(columnIndex, (Long) o);
		} else if (type.equals(Float.class)) {
			ps.setFloat(columnIndex, (Float) o);
		} else if (type.equals(Double.class)) {
			ps.setDouble(columnIndex, (Double) o);
		} else if (type.equals(byte[].class)) {
			ps.setBytes(columnIndex, (byte[]) o);
		} else if (type.equals(java.sql.Date.class)) {
			ps.setDate(columnIndex, (java.sql.Date) o);
		} else if (type.equals(Time.class)) {
			ps.setTime(columnIndex, (Time) o);
		} else if (type.equals(Timestamp.class)) {
			ps.setTimestamp(columnIndex, (Timestamp) o);
		} else if (type.equals(Clob.class)) {
			ps.setClob(columnIndex, (Clob) o);
		} else if (type.equals(Blob.class)) {
			ps.setBlob(columnIndex, (Blob) o);
		} else if (type.equals(Array.class)) {
			ps.setArray(columnIndex, (Array) o);
		} else if (type.equals(Ref.class)) {
			ps.setRef(columnIndex, (Ref) o);
		} else if (type.equals(URL.class)) {
			ps.setURL(columnIndex, (URL) o);
		}
	}

	public interface SqlFragment extends PreparedStatementBinder {
		void appendTo(SqlStringBuilder w);

		@Override
		default void bind(PreparedStatement ps, IntSequence index) throws SQLException {
			// do nothing
		}

		default String getSql() {
			final SqlStringBuilder builder = new SqlStringBuilder();
			appendTo(builder);
			return builder.getSql();
		}

	}

	public static class InSubqueryConditionBuilder implements Condition {
		private final SqlFragment rhs;
		private final SqlFragment lhs;

		public InSubqueryConditionBuilder(SqlFragment lhs, SqlFragment rhs) {
			this.lhs = lhs;
			this.rhs = rhs;
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			w.append(lhs);
			w.appendln(" in (");
			w.increaseIndent();
			w.append(rhs);
			w.decreaseIndent();
			w.append(")");
		}

		@Override
		public void bind(PreparedStatement ps, IntSequence index) throws SQLException {
			lhs.bind(ps, index);
			rhs.bind(ps, index);
		}
	}

	public static class SimpleConditionBuilder implements Condition {
		private final SqlFragment rhs;
		private final RelationalOperator operator;
		private final SqlFragment lhs;

		public SimpleConditionBuilder(SqlFragment lhs, RelationalOperator operator, SqlFragment rhs) {
			this.lhs = lhs;
			this.rhs = rhs;
			this.operator = operator;
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			lhs.appendTo(w);
			w.append(" ");
			operator.appendTo(w);
			w.append(" ");
			rhs.appendTo(w);
		}

		@Override
		public void bind(PreparedStatement ps, IntSequence index) throws SQLException {
			lhs.bind(ps, index);
			rhs.bind(ps, index);
		}
	}

	public enum LogicalOperator implements SqlFragment {
		AND("and"), OR("or");

		private final String sql;

		private LogicalOperator(String sql) {
			this.sql = sql;
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			w.append(sql);
		}
	}

	public static class CompositeCondition implements Condition {
		private final Collection<Condition> conditions;
		private final LogicalOperator operator;

		public CompositeCondition(Collection<Condition> conditions, LogicalOperator operator) {
			this.conditions = conditions;
			this.operator = operator;
		}

		@Override
		public void bind(PreparedStatement ps, IntSequence index) throws SQLException {
			for (final Condition c : conditions) {
				c.bind(ps, index);
			}
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			if (conditions.size() > 1) w.append("(");
			forEach_endAware(conditions, (c, first, last) -> {
				w.append(c);
				if (!last) w.append(" ").append(operator).append(" ");
			});
			if (conditions.size() > 1) w.append(")");
		}

	}

	public class ConditionBuilder<P> implements Condition {
		private Condition currentCondition;
		private final SqlFragment lhs;
		private P parent;

		public ConditionBuilder(String _lhs, P parent) {
			this.lhs = new SqlRaw(_lhs);
			this.parent = parent;
		}

		public void setParent(P parent) {
			this.parent = parent;
		}
		public ExpressionBuilder<P> is(RelationalOperator _operator) {
			final ExpressionBuilder<P> rhs = new ExpressionBuilder<>(parent);
			currentCondition = new SimpleConditionBuilder(lhs, _operator, rhs);
			return rhs;
		}

		// @formatter:off
		public ExpressionBuilder<P> eq() { return is(RelationalOperator.EQ); }
		public ExpressionBuilder<P> notEq() { return is(RelationalOperator.NOT_EQ); }
		public ExpressionBuilder<P> gt() { return is(RelationalOperator.GT); }
		public ExpressionBuilder<P> lt() { return is(RelationalOperator.LT); }
		public ExpressionBuilder<P> gte() { return is(RelationalOperator.GTE); }
		public ExpressionBuilder<P> lte() { return is(RelationalOperator.LTE); }
		// @formatter:on

		private <U> ConditionBuilder<P> _in(Collection<? extends U> values, Class<U> type) {
			final InConditionBuilder res = new InConditionBuilder(lhs, values, type);
			currentCondition = res;
			return this;
		}

		// Because of type erasure of generics we cannot have polymorphism here
		// @formatter:off
		public P in_String(Collection<? extends String> values) { _in(values, String.class); return parent; }
		public P in_BigDecimal(Collection<? extends BigDecimal> values) { _in(values, BigDecimal.class); return parent; }
		public P in_Boolean(Collection<? extends Boolean> values) { _in(values, Boolean.class); return parent; }
		public P in_Integer(Collection<? extends Integer> values) { _in(values, Integer.class); return parent; }
		public P in_Long(Collection<? extends Long> values) { _in(values, Long.class); return parent; }
		public P in_Float(Collection<? extends Float> values) { _in(values, Float.class); return parent; }
		public P in_Double(Collection<? extends Double> values) { _in(values, Double.class); return parent; }
		public P in_bytes(Collection<? extends byte[]> values) { _in(values, byte[].class); return parent; }
		public P in_Date(Collection<? extends java.sql.Date> values) { _in(values, java.sql.Date.class); return parent; }
		public P in_Time(Collection<? extends Time> values) { _in(values, Time.class); return parent; }
		public P in_Timestamp(Collection<? extends Timestamp> values) { _in(values, Timestamp.class); return parent; }
		public P in_Clob(Collection<? extends Clob> values) { _in(values, Clob.class); return parent; }
		public P in_Blob(Collection<? extends Blob> values) { _in(values, Blob.class); return parent; }
		public P in_Array(Collection<? extends Array> values) { _in(values, Array.class); return parent; }
		public P in_Ref(Collection<? extends Ref> values) { _in(values, Ref.class); return parent; }
		public P in_URL(Collection<? extends URL> values) { _in(values, URL.class); return parent; }
		// @formatter:on

		public P in(SqlSelectBuilder subquery) {
			currentCondition = new InSubqueryConditionBuilder(lhs, subquery);
			return parent;
		}

		public P isNull() {
			currentCondition = new SimpleConditionBuilder(lhs, RelationalOperator.IS, new SqlRaw("null"));
			return parent;
		}

		public P isNotNull() {
			currentCondition = new SimpleConditionBuilder(lhs, RelationalOperator.IS_NOT, new SqlRaw("null"));
			return parent;
		}

		public P like(String text, char escapeChar) {
			// TODO use placeholder instead
			currentCondition = new SimpleConditionBuilder(lhs, RelationalOperator.LIKE,
					new SqlRaw(OracleSqlUtils.escapeLikeString(text, escapeChar) + "'"));
			return parent;
		}

		public P like(String text) {
			// TODO use placeholder instead
			currentCondition = new SimpleConditionBuilder(lhs, RelationalOperator.LIKE,
					new SqlRaw(OracleSqlUtils.toLiteralString(text)));
			return parent;
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			w.append(currentCondition);
		}

		@Override
		public void bind(PreparedStatement ps, IntSequence index) throws SQLException {
			currentCondition.bind(ps, index);
		}
	}

	public enum JoinType implements SqlFragment {
		INNER("inner join"), LEFT("left join"), RIGHT("right join"), FULL("full join");

		private final String sql;

		JoinType(String sql) {
			this.sql = sql;
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			w.append(sql);
		}
	}

	public interface Condition extends SqlFragment {
		// tag interface
	}

	public abstract class SqlStatement implements SqlFragment {
		public final StatementOperation toStatement() {
			return new StatementOperation(cnxProvider, getSql(), this);
		}
	}

	public class SqlDeleteBuilder extends SqlStatement {
		private final String fromClause;
		private final Collection<SqlFragment> whereClauses = new ArrayList<>();

		public SqlDeleteBuilder(String fromClause) {
			this.fromClause = fromClause;
		}

		public ConditionBuilder<SqlDeleteBuilder> where(String lhs) {
			if (lhs == null) throw new IllegalArgumentException();
			final ConditionBuilder<SqlDeleteBuilder> res = new ConditionBuilder<>(lhs, this);
			whereClauses.add(res);
			return res;
		}

		public SqlDeleteBuilder where(Condition condition) {
			if (condition == null) throw new IllegalArgumentException();
			whereClauses.add(condition);
			return this;
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			w.append("delete ").append("from ").appendln(fromClause);
			if (!whereClauses.isEmpty()) {
				w.appendln("where");
				w.increaseIndent();
				forEach_endAware(whereClauses, (clause, first, last) -> {
					w.appendln(clause);
					if (!last) w.append("and ");
				});
				w.decreaseIndent();
			}
		}

		@Override
		public void bind(PreparedStatement ps, IntSequence index) throws SQLException {
			final CompositeIterator<SqlFragment> fragmentsIterator =
					new CompositeIterator<>(Arrays.asList(whereClauses.iterator()));
			while (fragmentsIterator.hasNext()) {
				fragmentsIterator.next().bind(ps, index);
			}
		}

		@Override
		public String toString() {
			return getSql();
		}
	}

	public static class WithClauseBuilder implements SqlFragment {
		private final String pseudoTableName;
		private SqlSelectBuilder subquery;
		private final SqlSelectBuilder parent;

		public WithClauseBuilder(String pseudoTableName, SqlSelectBuilder parent) {
			this.pseudoTableName = pseudoTableName;
			this.parent = parent;
		}

		public SqlSelectBuilder as(SqlSelectBuilder _subquery) {
			this.subquery = _subquery;
			return parent;
		}

		@Override
		public void bind(PreparedStatement ps, IntSequence index) throws SQLException {
			subquery.bind(ps, index);
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			w.append(pseudoTableName).appendln(" as (");
			w.increaseIndent();
			w.append(subquery);
			w.decreaseIndent();
			w.append(")");
		}
	}

	public abstract class SqlSelectStatement implements SqlFragment {
		public <T> Query<T> toQuery(ResultSetExtractor<T> extractor) {
			return new Query<T>(cnxProvider, getSql(), this, extractor);
		}
	}

	public class SqlSelectBuilder extends SqlSelectStatement {
		private boolean distinct;
		private final Collection<WithClauseBuilder> withClauses = new ArrayList<>();
		private final Collection<SqlFragment> selects = new ArrayList<>();
		private final Collection<String> joinClauses = new ArrayList<>();
		private final Collection<String> groupByClauses = new ArrayList<>();
		private final Collection<String> orderByClauses = new ArrayList<>();
		private final Collection<SqlFragment> whereClauses = new ArrayList<>();
		private final Collection<SqlFragment> havingClauses = new ArrayList<>();
		private String fromClause;

		public SqlSelectBuilder distinct() {
			distinct = true;
			return this;
		}

		public WithClauseBuilder with(String pseudoTableName) {
			final WithClauseBuilder res = new WithClauseBuilder(pseudoTableName, this);
			withClauses.add(res);
			return res;
		}

		public SqlSelectBuilder selectValue(String value) {
			selects.add(new SqlRaw(OracleSqlUtils.toLiteralString(value)));
			return this;
		}

		public SqlSelectBuilder select(String _selectClause) {
			selects.add(new SqlRaw(_selectClause));
			return this;
		}

		public SqlSelectBuilder select(String... _selects) {
			return select(Arrays.asList(_selects));
		}

		public SqlSelectBuilder select(Collection<String> _selects) {
			this.selects.addAll(_selects.stream().map(SqlRaw::new).collect(Collectors.toList()));
			return this;
		}

		public SqlSelectBuilder from(String _fromClause) {
			if (fromClause != null) throw new IllegalStateException();
			this.fromClause = _fromClause;
			return this;
		}

		private SqlSelectBuilder join(JoinType joinType, String joinClause) {
			if (joinClause == null) throw new IllegalArgumentException();
			joinClauses.add(joinType.getSql() + " " + joinClause);
			return this;
		}

		public SqlSelectBuilder innerJoin(String joinClause) {
			return join(JoinType.INNER, joinClause);
		}

		public SqlSelectBuilder leftJoin(String joinClause) {
			return join(JoinType.LEFT, joinClause);
		}

		public SqlSelectBuilder rightJoin(String joinClause) {
			return join(JoinType.RIGHT, joinClause);
		}

		public SqlSelectBuilder fullJoin(String joinClause) {
			return join(JoinType.FULL, joinClause);
		}

		public ConditionBuilder<SqlSelectBuilder> where(String lhs) {
			if (lhs == null) throw new IllegalArgumentException();
			final ConditionBuilder<SqlSelectBuilder> res = new ConditionBuilder<>(lhs, this);
			whereClauses.add(res);
			return res;
		}

		public SqlSelectBuilder where(Condition condition) {
			if (condition == null) throw new IllegalArgumentException();
			whereClauses.add(condition);
			return this;
		}

		public ConditionBuilder<SqlSelectBuilder> having(String lhs) {
			if (lhs == null) throw new IllegalArgumentException();
			final ConditionBuilder<SqlSelectBuilder> res = new ConditionBuilder<>(lhs, this);
			havingClauses.add(res);
			return res;
		}

		public SqlSelectBuilder having(Condition condition) {
			havingClauses.add(condition);
			return this;
		}

		public SqlSelectBuilder groupBy(String... _groupByClauses) {
			groupByClauses.addAll(Arrays.asList(_groupByClauses));
			return this;
		}

		public SqlSelectBuilder orderBy(String... _orderByClauses) {
			orderByClauses.addAll(Arrays.asList(_orderByClauses));
			return this;
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			if (!withClauses.isEmpty()) {
				w.append("with ");
				w.appendln(withClauses.stream().map(SqlFragment::getSql).collect(Collectors.joining(",\n")));
			}
			w.append("select ");
			if (distinct) w.append("distinct ");
			w.appendln(selects.stream().map(SqlFragment::getSql).collect(Collectors.joining(", ")));
			w.append("from ").appendln(fromClause);
			joinClauses.stream().forEach(i -> w.appendln(i));
			if (!whereClauses.isEmpty()) {
				w.appendln("where");
				w.increaseIndent();
				forEach_endAware(whereClauses, (clause, first, last) -> {
					w.appendln(clause);
					if (!last) w.append("and ");
				});
				w.decreaseIndent();
			}
			if (!groupByClauses.isEmpty()) {
				final String groupBy_str = groupByClauses.stream().collect(Collectors.joining(", "));
				w.append("group by ").appendln(groupBy_str);
			}
			if (!havingClauses.isEmpty()) {
				w.appendln("having");
				w.increaseIndent();
				forEach_endAware(havingClauses, (clause, first, last) -> {
					w.appendln(clause);
					if (!last) w.append("and ");
				});
				w.decreaseIndent();
			}
			if (!orderByClauses.isEmpty()) {
				final String orderBy_str = orderByClauses.stream().collect(Collectors.joining(", "));
				w.append("order by ").appendln(orderBy_str);
			}
		}

		@Override
		public void bind(PreparedStatement ps, IntSequence index) throws SQLException {
			final CompositeIterator<SqlFragment> fragmentsIterator = new CompositeIterator<>(
					Arrays.asList(selects.iterator(), whereClauses.iterator(), havingClauses.iterator()));
			while (fragmentsIterator.hasNext()) {
				fragmentsIterator.next().bind(ps, index);
			}
		}

		@Override
		public String toString() {
			return getSql();
		}

	}

	public static class SetValueClause implements SqlFragment {
		private final String columnName;
		private final ExpressionBuilder<InsertValuesBuilder> value;

		public SetValueClause(String columnName, ExpressionBuilder<InsertValuesBuilder> value) {
			this.columnName = columnName;
			this.value = value;
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			w.append(columnName).append(" = ").append(value);
		}

		public String getColumnName() {
			return columnName;
		}

		public ExpressionBuilder<InsertValuesBuilder> getValue() {
			return value;
		}

		@Override
		public void bind(PreparedStatement st, IntSequence index) throws SQLException {
			value.bind(st, index);
		}
	}

	public static class UpdateSetClause implements SqlFragment {
		private final String columnName;
		private final ExpressionBuilder<SqlUpdateBuilder> value;

		public UpdateSetClause(String columnName, ExpressionBuilder<SqlUpdateBuilder> value) {
			this.columnName = columnName;
			this.value = value;
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			w.append(columnName).append(" = ").append(value);
		}

		public String getColumnName() {
			return columnName;
		}

		public ExpressionBuilder<SqlUpdateBuilder> getValue() {
			return value;
		}

		@Override
		public void bind(PreparedStatement st, IntSequence index) throws SQLException {
			value.bind(st, index);
		}
	}

	public class InsertValuesBuilder implements SqlFragment {
		private final Collection<SetValueClause> setClauses = new ArrayList<>();

		public InsertValuesBuilder() {
			this(Collections.emptyList());
		}

		public InsertValuesBuilder(Collection<? extends SetValueClause> _setClauses) {
			setClauses.addAll(_setClauses);
		}

		public ExpressionBuilder<InsertValuesBuilder> set(String columnName) {
			final ExpressionBuilder<InsertValuesBuilder> res = new ExpressionBuilder<>(this);
			setClauses.add(new SetValueClause(columnName, res));
			return res;
		}

		@Override
		public void bind(PreparedStatement ps, IntSequence index) throws SQLException {
			for (final SetValueClause clause : setClauses) {
				clause.bind(ps, index);
			}
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			w.append("(");
			w.append(setClauses.stream().map(SetValueClause::getColumnName).collect(Collectors.joining(", ")));
			w.append(")");
			w.appendln();
			w.append("values (");
			w.append(
					setClauses.stream().map(SetValueClause::getValue).map(SqlFragment::getSql).collect(
							Collectors.joining(", ")));
			w.append(")");
		}

	}

	public class SqlInsertBuilder extends SqlStatement {
		private final String tableName;
		private SqlFragment body;
		private Collection<String> columns;

		public SqlInsertBuilder(String tableName) {
			this.tableName = tableName;
		}

		public SqlSelectBuilder subquery(String... _columns) {
			if (body != null) throw new IllegalArgumentException();
			this.columns = Arrays.asList(_columns);
			final SqlSelectBuilder res = new SqlSelectBuilder();
			body = res;
			return res;
		}

		public InsertValuesBuilder values() {
			if (body instanceof InsertValuesBuilder) return (InsertValuesBuilder) body;
			if (body != null) throw new IllegalArgumentException();

			final InsertValuesBuilder res = new InsertValuesBuilder();
			body = res;
			return res;
		}

		@Override
		public void bind(PreparedStatement ps, IntSequence index) throws SQLException {
			body.bind(ps, index);
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			w.append("insert into ").append(tableName);
			if (columns != null) {
				w.append(" (");
				w.append(columns.stream().collect(Collectors.joining(", ")));
				w.appendln(")");
			}
			w.append(body);
		}

	}

	public class SqlUpdateBuilder extends SqlStatement {
		private final Collection<SqlFragment> whereClauses = new ArrayList<>();
		private final Collection<UpdateSetClause> setClauses = new ArrayList<>();
		private final String tableName;

		public SqlUpdateBuilder(String tableName) {
			this.tableName = tableName;
		}

		public ExpressionBuilder<SqlUpdateBuilder> set(String columnName) {
			final ExpressionBuilder<SqlUpdateBuilder> res = new ExpressionBuilder<>(this);
			setClauses.add(new UpdateSetClause(columnName, res));
			return res;
		}

		public ConditionBuilder<SqlUpdateBuilder> where(String lhs) {
			if (lhs == null) throw new IllegalArgumentException();
			final ConditionBuilder<SqlUpdateBuilder> res = new ConditionBuilder<>(lhs, this);
			whereClauses.add(res);
			return res;
		}

		public SqlUpdateBuilder where(Condition condition) {
			if (condition == null) throw new IllegalArgumentException();
			whereClauses.add(condition);
			return this;
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			w.append("update ").append(tableName).appendln(" set");
			w.increaseIndent();
			forEach_endAware(setClauses, (clause, first, last) -> {
				w.append(clause);
				w.appendln(last ? "" : ",");
			});
			w.decreaseIndent();

			if (!whereClauses.isEmpty()) {
				w.appendln("where");
				w.increaseIndent();
				forEach_endAware(whereClauses, (clause, first, last) -> {
					w.appendln(clause);
					if (!last) w.append("and ");
				});
				w.decreaseIndent();
			}
		}

		@Override
		public void bind(PreparedStatement ps, IntSequence index) throws SQLException {
			final CompositeIterator<SqlFragment> fragmentsIterator =
					new CompositeIterator<>(Arrays.asList(setClauses.iterator(), whereClauses.iterator()));
			while (fragmentsIterator.hasNext()) {
				fragmentsIterator.next().bind(ps, index);
			}
		}
	}

	public enum SqlMergeClauseFlag {
		ON_CLAUSE, UPDATE_CLAUSE, INSERT_CLAUSE
	}

	public static class SqlMergeClause implements SqlFragment {
		private final EnumSet<SqlMergeClauseFlag> flags;
		private final String columnName;
		private final ExpressionBuilder<SqlMergeBuilder> value;

		public SqlMergeClause(EnumSet<SqlMergeClauseFlag> flags, String columnName,
				ExpressionBuilder<SqlMergeBuilder> value) {
			this.flags = flags;
			this.columnName = columnName;
			this.value = value;
		}

		@Override
		public void bind(PreparedStatement ps, IntSequence index) throws SQLException {
			value.bind(ps, index);
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			w.append(columnName).append(" = ").append(value);
		}

		public EnumSet<SqlMergeClauseFlag> getFlags() {
			return flags;
		}

		public String getColumnName() {
			return columnName;
		}

		public ExpressionBuilder<SqlMergeBuilder> getValue() {
			return value;
		}
	}

	public class SqlMergeBuilder extends SqlStatement {

		private final String tableName;
		private final Collection<SqlMergeClause> setClauses = new ArrayList<>();

		public SqlMergeBuilder(String tableName) {
			this.tableName = tableName;
		}

		public ExpressionBuilder<SqlMergeBuilder> on(String columnName) {
			final ExpressionBuilder<SqlMergeBuilder> res = new ExpressionBuilder<>(this);
			setClauses.add(
					new SqlMergeClause(EnumSet.of(SqlMergeClauseFlag.ON_CLAUSE, SqlMergeClauseFlag.INSERT_CLAUSE),
							columnName, res));
			return res;
		}

		public ExpressionBuilder<SqlMergeBuilder> insertOrUpdate(String columnName) {
			final ExpressionBuilder<SqlMergeBuilder> res = new ExpressionBuilder<>(this);
			setClauses.add(
					new SqlMergeClause(EnumSet.of(SqlMergeClauseFlag.UPDATE_CLAUSE, SqlMergeClauseFlag.INSERT_CLAUSE),
							columnName, res));
			return res;
		}

		public ExpressionBuilder<SqlMergeBuilder> insert(String columnName) {
			final ExpressionBuilder<SqlMergeBuilder> res = new ExpressionBuilder<>(this);
			setClauses.add(new SqlMergeClause(EnumSet.of(SqlMergeClauseFlag.INSERT_CLAUSE), columnName, res));
			return res;
		}

		@Override
		public void bind(PreparedStatement ps, IntSequence index) throws SQLException {
			final List<SqlMergeClause> onClauses =
					setClauses.stream().filter(c -> c.getFlags().contains(SqlMergeClauseFlag.ON_CLAUSE)).collect(
							Collectors.toList());
			final List<SqlMergeClause> updateClauses =
					setClauses.stream().filter(c -> c.getFlags().contains(SqlMergeClauseFlag.UPDATE_CLAUSE)).collect(
							Collectors.toList());
			final List<SqlMergeClause> insertClauses =
					setClauses.stream().filter(c -> c.getFlags().contains(SqlMergeClauseFlag.INSERT_CLAUSE)).collect(
							Collectors.toList());

			final CompositeIterator<SqlMergeClause> it = new CompositeIterator<>(
					Arrays.asList(onClauses.iterator(), updateClauses.iterator(), insertClauses.iterator()));
			while (it.hasNext()) {
				it.next().bind(ps, index);
			}
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			final List<SqlMergeClause> onClauses =
					setClauses.stream().filter(c -> c.getFlags().contains(SqlMergeClauseFlag.ON_CLAUSE)).collect(
							Collectors.toList());
			final List<SqlMergeClause> updateClauses =
					setClauses.stream().filter(c -> c.getFlags().contains(SqlMergeClauseFlag.UPDATE_CLAUSE)).collect(
							Collectors.toList());
			final List<SqlMergeClause> insertClauses =
					setClauses.stream().filter(c -> c.getFlags().contains(SqlMergeClauseFlag.INSERT_CLAUSE)).collect(
							Collectors.toList());

			w.append("merge into ").append(tableName).appendln(" using dual on (");
			w.increaseIndent();
			forEach_endAware(onClauses, (clause, first, last) -> {
				if (!first) w.append("and ");
				w.appendln(clause);
			});
			w.decreaseIndent();
			w.appendln(")");
			if (!updateClauses.isEmpty()) {
				w.appendln("when matched then update set");
				w.increaseIndent();
				forEach_endAware(updateClauses, (clause, first, last) -> {
					w.append(clause);
					w.appendln(last ? "" : ",");
				});
				w.decreaseIndent();
			}
			w.append("when not matched then insert (");
			w.append(insertClauses.stream().map(SqlMergeClause::getColumnName).collect(Collectors.joining(", ")));
			w.append(") values (");
			w.append(
					insertClauses.stream().map(SqlMergeClause::getValue).map(SqlFragment::getSql).collect(
							Collectors.joining(", ")));
			w.append(")");
		}

	}

	@FunctionalInterface
	private interface EndAwareConsumer<T> {
		void accept(T value, boolean first, boolean last);
	}

	private static <T> void forEach_endAware(Collection<? extends T> collection, EndAwareConsumer<T> consumer) {
		final int count = collection.size();
		int i = 0;
		for (final T t : collection) {
			consumer.accept(t, i == 0, i == count - 1);
			i++;
		}
	}

	public class CompositeSqlSelectStatement extends SqlSelectStatement {
		private final Collection<SqlSelectStatement> fragments;
		private final String joinClause;

		public CompositeSqlSelectStatement(Collection<SqlSelectStatement> fragments, String joinClause) {
			this.fragments = fragments;
			this.joinClause = joinClause;
		}

		@Override
		public void bind(PreparedStatement ps, IntSequence index) throws SQLException {
			for (final SqlFragment fragment : fragments) {
				fragment.bind(ps, index);
			}
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			forEach_endAware(fragments, (fragment, first, last) -> {
				w.append(fragment);
				if (!last) w.append(joinClause);
			});
		}

	}

	/**
	 * Represent a batch statement. Unlike in JDBC where a batch statement is
	 * represented by a single {@link java.sql.Statement} object, here the
	 * BatchStatement holds a collection of {@link SqlStatement} items.
	 * <p>
	 * Warning: each {@link SqlStatement} item must represent the same SQL
	 * statement. Only the first statement will be converted to SQL.
	 * <p>
	 * At the moment, there is no check to actually make sure the SQL is the
	 * same. This may change in the future.
	 *
	 */
	public class BatchStatementBuilder extends SqlStatement {
		private final Collection<SqlStatement> statements;

		public BatchStatementBuilder() {
			this.statements = new ArrayList<>();
		}

		public BatchStatementBuilder(Collection<? extends SqlStatement> statements) {
			this.statements = new ArrayList<>(statements);
		}

		public void addStatement(SqlStatement statement) {
			statements.add(statement);
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			if (statements.isEmpty()) return;
			statements.iterator().next().appendTo(w);
		}

		@Override
		public void bind(PreparedStatement ps, IntSequence index) throws SQLException {
			for (final SqlStatement s : statements) {
				s.bind(ps, index);
				ps.addBatch();
				index.reset();
			}
		}

	}

	/**
	 * Create a batch statement from an SQL string and a stream of prepared
	 * statement binders.
	 */
	public class StreamBackedBatchStatement extends SqlStatement {
		private final Stream<? extends PreparedStatementBinder> statements;
		private final String sql;

		public StreamBackedBatchStatement(String sql, Stream<? extends PreparedStatementBinder> statements) {
			this.sql = sql;
			this.statements = statements;
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			w.append(sql);
		}

		@Override
		public void bind(PreparedStatement ps, IntSequence index) throws SQLException {
			statements.forEachOrdered(s -> {
				try {
					s.bind(ps, index);
					ps.addBatch();
				} catch (final SQLException e) {
					throw new FjdbcException(e);
				}
				index.reset();
			});
		}
	}

	/**
	 * Debug statements by printing the value of prepared values in a comment
	 * next to the '?' placeholder.
	 */
	public boolean isDebug() {
		return debug;
	}
}
