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
import com.github.fjdbc.IntSequence;
import com.github.fjdbc.PreparedStatementBinder;
import com.github.fjdbc.internal.PreparedStatementEx;
import com.github.fjdbc.internal.StatementOperationImpl;
import com.github.fjdbc.op.StatementOperation;
import com.github.fjdbc.query.Query;
import com.github.fjdbc.query.ResultSetExtractor;

/**
 * SQL generator using a fluent interface.
 * <p>
 * It provides methods to generate the following types of statements:
 * <ul>
 * <li>SELECT
 * <li>INSERT
 * <li>UPDATE
 * <li>DELETE
 * <li>MERGE
 * </ul>
 * <p>
 * The reference source used for the syntax is the
 * <a href="https://ronsavage.github.io/SQL/sql-2003-2.bnf.html">SQL-2003 specification</a>.
 * <p>
 * The following syntactic elements are supported:
 * <table border="1" summary="">
 * <tr>
 * <td>Element</td>
 * <td>Clause</td>
 * <td>Supported?</td>
 * </tr>
 * <tr>
 * <td>SELECT</td>
 * <td>GROUP BY</td>
 * <td>Supported</td>
 * </tr>
 * <tr>
 * <td>SELECT</td>
 * <td>HAVING</td>
 * <td>Supported</td>
 * </tr>
 * <tr>
 * <td>SELECT</td>
 * <td>ORDER BY</td>
 * <td>Supported</td>
 * </tr>
 * <tr>
 * <td>SELECT</td>
 * <td>WITH ... SELECT ...</td>
 * <td>Supported</td>
 * </tr>
 * <tr>
 * <td>SELECT</td>
 * <td>WITH RECURSIVE ... SELECT ...</td>
 * <td>Not supported</td>
 * </tr>
 * <tr>
 * <td>SELECT</td>
 * <td>SELECT ... INTO ...</td>
 * <td>Not supported</td>
 * </tr>
 * <tr>
 * <td>SELECT</td>
 * <td>SELECT DISTINCT</td>
 * <td>Supported</td>
 * </tr>
 * <tr>
 * <td>SELECT</td>
 * <td>SELECT ALL</td>
 * <td>Not supported</td>
 * </tr>
 * <tr>
 * <td>SELECT</td>
 * <td>WINDOW</td>
 * <td>Not supported</td>
 * </tr>
 * <tr>
 * <td>SELECT</td>
 * <td>INNER|LEFT|RIGHT|FULL|CROSS JOIN</td>
 * <td>Supported</td>
 * </tr>
 * <tr>
 * <td>SELECT</td>
 * <td>NATURAL JOIN</td>
 * <td>Not supported</td>
 * </tr>
 * <tr>
 * <td>SELECT</td>
 * <td>UNION JOIN</td>
 * <td>Not supported</td>
 * </tr>
 * <tr>
 * <td>SELECT</td>
 * <td>SELECT ... FROM ... TABLESAMPLE ...</td>
 * <td>Supported</td>
 * </tr>
 * <tr>
 * <td>SELECT</td>
 * <td>UNION CORRESPONDING</td>
 * <td>Not supported</td>
 * </tr>
 * </table>
 */
public class SqlBuilder {
	/**
	 * Debug statements by printing the value of prepared values in a comment next to the '?' placeholder.
	 */
	private final boolean debug;
	private final ConnectionProvider cnxProvider;
	private final SqlDialect dialect;

	/**
	 * @param cnxProvider
	 *        The database connection provider.
	 */
	public SqlBuilder(ConnectionProvider cnxProvider) {
		this(cnxProvider, SqlDialect.STANDARD, false);
	}

	/**
	 * @param cnxProvider
	 *        The database connection provider.
	 * @param debug
	 *        Debug statements by printing the value of prepared values in a comment next to the '?' placeholder.
	 */
	public SqlBuilder(ConnectionProvider cnxProvider, SqlDialect dialect, boolean debug) {
		this.cnxProvider = cnxProvider;
		this.dialect = dialect;
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
		res.select(_selects);
		return res;
	}

	/**
	 * Convenience method to build a {@code SELECT DISTINCT} statement.
	 */
	public SqlSelectBuilder selectDistinct(String... _selects) {
		final SqlSelectBuilder res = new SqlSelectBuilder().distinct();
		res.select(_selects);
		return res;
	}

	/**
	 * Build a {@code SELECT} statement that starts with a {@code WITH} clause.
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
	 * Build a {@code MERGE} statement.
	 */
	public SqlMergeBuilder mergeInto(String tableName) {
		return new SqlMergeBuilder(tableName);
	}

	/**
	 * Build an SQL condition, to use in {@code WHERE} clauses for instance.
	 * @param lhs
	 *        The left-hand side of the condition.
	 */
	public ConditionBuilder<Condition> condition(String lhs) {
		final ConditionBuilder<Condition> res = new ConditionBuilder<>(lhs, null);
		res.setParent(res);
		return res;
	}

	/**
	 * Build an SQL condition that is either always true, or always false.
	 * <p>
	 * It is implemented by outputting either '1 = 1' or '1 = 0'.
	 */
	public Condition bool(boolean value) {
		final int value_int = value ? 1 : 0;
		return new SimpleConditionBuilder(new SqlParameter<>(1, Integer.class), RelationalOperator.EQ,
				new SqlParameter<>(value_int, Integer.class));
	}

	/**
	 * Convert an arbitrary string to an SQL fragment.
	 * <p>
	 * The SQL fragment may then be used in any of the {@code raw} methods, or as an SQL {@code Condition}.
	 */

	public SqlRaw raw(String sql) {
		return new SqlRaw(sql);
	}

	/**
	 * Convert an arbitrary string and a prepared statement binder to an SQL fragment.
	 * <p>
	 * The SQL fragment may then be used in any of the {@code raw} methods, or as an SQL {@code Condition}.
	 */
	public SqlRaw raw(String sql, PreparedStatementBinder binder) {
		return new SqlRaw(sql, binder);
	}

	public SqlRaw raw(String sql, Object value1) {
		final PreparedStatementBinder binder = PreparedStatementBinder.create(value1);
		return new SqlRaw(sql, binder);
	}

	/**
	 * Build an {@code AND} condition.
	 * @param conditions
	 *        The conditions to be joined with the {@code AND} operator.
	 */
	public CompositeConditionBuilder and(Condition... conditions) {
		return new CompositeConditionBuilder(Arrays.asList(conditions), LogicalOperator.AND);
	}

	/**
	 * Build an {@code OR} condition.
	 * @param conditions
	 *        The conditions to be joined with the {@code OR} operator.
	 */
	public CompositeConditionBuilder or(Condition... conditions) {
		return new CompositeConditionBuilder(Arrays.asList(conditions), LogicalOperator.OR);
	}

	public Condition not(Condition condition) {
		return new NotCondition(condition);
	}

	public Condition exists(SqlSelectStatement subquery) {
		return new ExistsCondition(subquery);
	}

	/**
	 * Build a {@code SELECT} statement that is the {@code UNION} of all specified {@code SELECT} statements.
	 * @param selects
	 *        The {@code SELECT} statements to join.
	 */
	public SqlSelectStatement union(SqlSelectBuilder... selects) {
		return new CompositeSqlSelectStatement(Arrays.asList(selects), "union\n");
	}

	/**
	 * Build a {@code SELECT} statement that is the {@code UNION ALL} of all specified {@code SELECT} statements.
	 * @param selects
	 *        The {@code SELECT} statements to join.
	 */
	public SqlSelectStatement unionAll(SqlSelectBuilder... selects) {
		return new CompositeSqlSelectStatement(Arrays.asList(selects), "union all\n");
	}

	/**
	 * Build a {@code SELECT} statement that is the intersection of all specified {@code SELECT} statements.
	 * @param selects
	 *        The {@code SELECT} statements to join with the {@code INTERSECT} operator.
	 */
	public SqlSelectStatement intersect(SqlSelectBuilder... selects) {
		return new CompositeSqlSelectStatement(Arrays.asList(selects), "intersect\n");
	}

	/**
	 * Build a {@code SELECT} statement that represents {@code a MINUS b}.
	 * <p>
	 * Oracle-specific; use {@link #except} for standard SQL.
	 */
	public SqlSelectStatement minus(SqlSelectBuilder a, SqlSelectBuilder b) {
		return new CompositeSqlSelectStatement(Arrays.asList(a, b), "minus\n");
	}

	/**
	 * Build a {@code SELECT} statement that represents {@code a EXCEPT b}.
	 */
	public SqlSelectStatement except(SqlSelectBuilder a, SqlSelectBuilder b) {
		return new CompositeSqlSelectStatement(Arrays.asList(a, b), "except\n");
	}

	/**
	 * Build a batch statement. The batch statement is initially empty; statements must be added with the
	 * {@link BatchStatementBuilder#addStatement} method.
	 */
	public BatchStatementBuilder batch() {
		return new BatchStatementBuilder();
	}

	/**
	 * Build a batch statement from an SQL string and a stream of prepared statement binders.
	 */
	public <T extends SqlFragment> BatchStatementOperation<T> batchStatement(Stream<T> statements,
			long executeEveryNRow, long commitEveryNRow) {
		return new BatchStatementOperation<>(cnxProvider, statements, executeEveryNRow, commitEveryNRow);
	}

	/**
	 * Build a batch statement from an SQL string and a stream of prepared statement binders.
	 */
	public <T extends SqlFragment> BatchStatementOperation<T> batchStatement(Stream<T> statements) {
		return batchStatement(statements, -1, -1);
	}

	/**
	 * Build a batch statement from an SQL string and a stream of prepared statement binders.
	 */
	public <T extends SqlFragment> BatchStatementOperation<T> batchStatement(Collection<T> statements) {
		return batchStatement(statements.stream(), -1, -1);
	}

	public enum RelationalOperator implements SqlFragment {
		EQ("="),
		NOT_EQ("<>"),
		GT(">"),
		GTE(">="),
		LT("<"),
		LTE("<="),
		LIKE("like"),
		IS("is"),
		IS_NOT("is not"),
		IN("in");

		private final String value;

		RelationalOperator(String value) {
			this.value = value;
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			w.append(value);
		}
	}

	public class InConditionBuilder implements Condition {
		private final String sql;
		private final PreparedStatementBinder binder;

		public <T> InConditionBuilder(SqlFragment lhs, Collection<? extends T> values, Class<T> type) {
			if (values == null) throw new IllegalArgumentException();

			if (values.isEmpty()) {
				sql = "1=0";
				binder = null;
			} else {
				final int maxItemsForInClause = 1000; // Oracle limit
				final ArrayList<String> sqlClauses = new ArrayList<>(values.size() / maxItemsForInClause + 1);
				final List<List<String>> subCollections = SqlUtils.partition(Collections.nCopies(values.size(), "?"),
						maxItemsForInClause);
				for (final List<String> subCollection : subCollections) {
					sqlClauses.add(
							lhs.getSql() + " in (" + subCollection.stream().collect(Collectors.joining(", ")) + ")");
				}
				sql = sqlClauses.size() == 1 ? sqlClauses.get(0)
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

		@Override
		public boolean representsNullValue() {
			return sql.trim().equalsIgnoreCase("NULL");
		}
		
		@Override
		public String getSql() {
			// as a small optimization, we provide the SQL directly since it is a constant value.
			return sql;
		}
	}

	public static class SqlStringBuilder {
		private final StringBuilder sb = new StringBuilder();
		private int indentLevel = 0;
		private boolean startLine = true;

		public SqlStringBuilder append(String sql) {
			if (startLine)
				sb.append(Collections.nCopies(indentLevel * 4, " ").stream().collect(Collectors.joining()));
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

	/**
	 * Represents a PreparedStatement parameter.
	 * @param <T>
	 *        The type of the PreparedStatement parameter.
	 */
	public class SqlParameter<T> implements SqlFragment {
		private final T value;
		private final Class<T> type;
		private final String sql;

		public SqlParameter(T value, Class<T> type) {
			this("?", value, type);
		}

		public SqlParameter(String rawSql, T value, Class<T> type) {
			this.sql = rawSql;
			assert rawSql != null && rawSql.contains("?");
			this.value = value;
			this.type = type;
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			w.append(sql);
			if (isDebug()) {
				w.append("  /* ");
				w.append(value == null ? "null" : SqlUtils.escapeComment(value.toString()));
				w.append(" */");
			}
		}

		@Override
		public void bind(PreparedStatement st, IntSequence index) throws SQLException {
			setAnyObject(st, index.next(), value, type);
		}

		@Override
		public boolean representsNullValue() {
			return value == null;
		}
	}

	public static class ExistsCondition extends CompositeSqlFragment implements Condition {

		public ExistsCondition(SqlSelectStatement wrapped) {
			super(new SqlRaw("exists ("), SqlFragment.newlineIndent, wrapped, SqlFragment.dedent, new SqlRaw(")"));
		}
	}

	public static class CompositeSqlFragment implements SqlFragment {
		private final Collection<SqlFragment> fragments;

		public CompositeSqlFragment(SqlFragment... fragments) {
			this.fragments = Arrays.asList(fragments);
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			for (final SqlFragment fragment : fragments) {
				w.append(fragment);
			}
		}

		@Override
		public void bind(PreparedStatement ps, IntSequence index) throws SQLException {
			for (final SqlFragment fragment : fragments) {
				fragment.bind(ps, index);
			}
		}
	}

	/**
	 * @param <P>
	 *        the parent type
	 */
	public class ExpressionBuilder<P> implements SqlFragment {
		private SqlFragment wrapped;
		private final P parent;

		public ExpressionBuilder(P parent) {
			this.parent = parent;
		}

		@SuppressWarnings("unchecked")
		public <T> P value(Object value, Class<T> _class) {
			if (value == null) {
				wrapped = new SqlParameter<>((T) value, _class);
			} else {
				if (!value.getClass().equals(_class))
					throw new IllegalArgumentException("Object must be of type: " + _class);
				if (!PreparedStatementEx.jdbcTypes.contains(_class))
					throw new IllegalArgumentException(String.format("Invalid JDBC type: %s. Allowed types are: %s",
							_class, PreparedStatementEx.jdbcTypes));
				wrapped = new SqlParameter<>((T) value, _class);
			}
			return parent;
		}

		// @formatter:off
		public P value(String value) { wrapped = new SqlParameter<>(value, String.class); return parent; }
		public P value(BigDecimal value) { wrapped = new SqlParameter<>(value, BigDecimal.class); return parent; }
		public P value(Boolean value) { wrapped = new SqlParameter<>(value, Boolean.class); return parent; }
		public P value(Integer value) { wrapped = new SqlParameter<>(value, Integer.class); return parent; }
		public P value(Long value) { wrapped = new SqlParameter<>(value, Long.class); return parent; }
		public P value(Float value) { wrapped = new SqlParameter<>(value, Float.class); return parent; }
		public P value(Double value) { wrapped = new SqlParameter<>(value, Double.class); return parent; }
		public P value(byte[] value) { wrapped = new SqlParameter<>(value, byte[].class); return parent; }
		public P value(java.sql.Date value) { wrapped = new SqlParameter<>(value, java.sql.Date.class); return parent; }
		public P value(Time value) { wrapped = new SqlParameter<>(value, Time.class); return parent; }
		public P value(Timestamp value) { wrapped = new SqlParameter<>(value, Timestamp.class); return parent; }
		public P value(Clob value) { wrapped = new SqlParameter<>(value, Clob.class); return parent; }
		public P value(Blob value) { wrapped = new SqlParameter<>(value, Blob.class); return parent; }
		public P value(Array value) { wrapped = new SqlParameter<>(value, Array.class); return parent; }
		public P value(Ref value) { wrapped = new SqlParameter<>(value, Ref.class); return parent; }
		public P value(URL value) { wrapped = new SqlParameter<>(value, URL.class); return parent; }
		
		public P raw(String sql, String value) { wrapped = new SqlParameter<>(sql, value, String.class); return parent; }
		public P raw(String sql, BigDecimal value) { wrapped = new SqlParameter<>(sql, value, BigDecimal.class); return parent; }
		public P raw(String sql, Boolean value) { wrapped = new SqlParameter<>(sql, value, Boolean.class); return parent; }
		public P raw(String sql, Integer value) { wrapped = new SqlParameter<>(sql, value, Integer.class); return parent; }
		public P raw(String sql, Long value) { wrapped = new SqlParameter<>(sql, value, Long.class); return parent; }
		public P raw(String sql, Float value) { wrapped = new SqlParameter<>(sql, value, Float.class); return parent; }
		public P raw(String sql, Double value) { wrapped = new SqlParameter<>(sql, value, Double.class); return parent; }
		public P raw(String sql, byte[] value) { wrapped = new SqlParameter<>(sql, value, byte[].class); return parent; }
		public P raw(String sql, java.sql.Date value) { wrapped = new SqlParameter<>(sql, value, java.sql.Date.class); return parent; }
		public P raw(String sql, Time value) { wrapped = new SqlParameter<>(sql, value, Time.class); return parent; }
		public P raw(String sql, Timestamp value) { wrapped = new SqlParameter<>(sql, value, Timestamp.class); return parent; }
		public P raw(String sql, Clob value) { wrapped = new SqlParameter<>(sql, value, Clob.class); return parent; }
		public P raw(String sql, Blob value) { wrapped = new SqlParameter<>(sql, value, Blob.class); return parent; }
		public P raw(String sql, Array value) { wrapped = new SqlParameter<>(sql, value, Array.class); return parent; }
		public P raw(String sql, Ref value) { wrapped = new SqlParameter<>(sql, value, Ref.class); return parent; }
		public P raw(String sql, URL value) { wrapped = new SqlParameter<>(sql, value, URL.class); return parent; }
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
		 *        A subquery that returns a single row.
		 */
		public P subquery(SqlSelectBuilder subquery) {
			wrapped = SqlFragment.wrapInParentheses(subquery, true);
			return parent;
		}

		public P all(SqlSelectBuilder subquery) {
			wrapped = new CompositeSqlFragment(new SqlRaw("all ("), SqlFragment.newlineIndent, subquery,
					SqlFragment.dedent, new SqlRaw(")"));
			return parent;
		}

		public P any(SqlSelectBuilder subquery) {
			wrapped = new CompositeSqlFragment(new SqlRaw("any ("), SqlFragment.newlineIndent, subquery,
					SqlFragment.dedent, new SqlRaw(")"));
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

		@Override
		public boolean representsNullValue() {
			return wrapped.representsNullValue();
		}
	}

	<T> void setAnyObject(PreparedStatement ps, int columnIndex, T o, Class<T> type) throws SQLException {
		if (o == null) {
			// java.sql.Types.OTHER does not work with Oracle driver.
			if (dialect == SqlDialect.ORACLE) {
				ps.setNull(columnIndex, java.sql.Types.INTEGER);
			} else {
				ps.setNull(columnIndex, java.sql.Types.OTHER);
			}

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

		public static final SqlFragment indent = w -> {
			w.increaseIndent();
		};
		public static final SqlFragment newlineIndent = w -> {
			w.appendln();
			w.increaseIndent();
		};

		public static final SqlFragment dedent = w -> {
			w.decreaseIndent();
		};

		public static final SqlFragment newlineDedent = w -> {
			w.appendln();
			w.decreaseIndent();
		};

		public static final SqlFragment nullLiteral = w -> w.append("NULL");

		public static SqlFragment wrapInParentheses(SqlFragment fragment, boolean newlineAndIndent) {
			return newlineAndIndent
					? new CompositeSqlFragment(new SqlRaw("("), SqlFragment.newlineIndent, fragment,
							SqlFragment.dedent, new SqlRaw(")"))
					: new CompositeSqlFragment(new SqlRaw("("), fragment, new SqlRaw(")"));
		}

		/**
		 * Returns true if this fragments represents the SQL {@code NULL} value.
		 */
		default boolean representsNullValue() {
			return false;
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
		private SqlFragment rhs;
		private RelationalOperator operator;
		private final SqlFragment lhs;
		private boolean fixNullRhs;

		public SimpleConditionBuilder(SqlFragment lhs, RelationalOperator operator, SqlFragment rhs) {
			this(lhs, operator, rhs, false);
		}

		public SimpleConditionBuilder(SqlFragment lhs, RelationalOperator operator, SqlFragment rhs,
				boolean fixNullRhs) {
			this.lhs = lhs;
			this.rhs = rhs;
			this.operator = operator;
			this.fixNullRhs = fixNullRhs;
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			fixNullRhs();
			lhs.appendTo(w);
			w.append(" ");
			operator.appendTo(w);
			w.append(" ");
			rhs.appendTo(w);
		}

		/**
		 * Handles the special case of NULL values on the RHS: is this case '=' is replaced with 'IS'.
		 */
		private void fixNullRhs() {
			if (!fixNullRhs) return;
			final boolean rhsNull = rhs.representsNullValue();
			if (fixNullRhs && rhsNull && operator == RelationalOperator.EQ) {
				operator = RelationalOperator.IS;
				rhs = SqlFragment.nullLiteral;
			} else if (fixNullRhs && rhsNull && operator == RelationalOperator.NOT_EQ) {
				operator = RelationalOperator.IS_NOT;
				rhs = SqlFragment.nullLiteral;
			}
			fixNullRhs = false;
		}

		@Override
		public void bind(PreparedStatement ps, IntSequence index) throws SQLException {
			fixNullRhs();
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

	public static class CompositeConditionBuilder implements Condition {
		private final Collection<Condition> conditions;
		private final LogicalOperator operator;

		public CompositeConditionBuilder(Collection<Condition> conditions, LogicalOperator operator) {
			this.conditions = new ArrayList<>(conditions);
			this.operator = operator;
		}

		public void add(Condition condition) {
			conditions.add(condition);
		}

		@Override
		public void bind(PreparedStatement ps, IntSequence index) throws SQLException {
			for (final Condition c : conditions) {
				c.bind(ps, index);
			}
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			if (conditions.size() == 0) {
				w.append("1=1");
			} else {
				if (conditions.size() > 1) w.append("(");
				forEach_endAware(conditions, (c, first, last) -> {
					w.append(c);
					if (!last) w.append(" ").append(operator).append(" ");
				});
				if (conditions.size() > 1) w.append(")");
			}
		}

	}

	public static class NotCondition implements Condition {
		private final Condition wrapped;

		public NotCondition(Condition condition) {
			assert condition != null;
			this.wrapped = condition;
		}

		@Override
		public void bind(PreparedStatement ps, IntSequence index) throws SQLException {
			wrapped.bind(ps, index);
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			w.appendln("not (");
			w.increaseIndent();
			w.appendln(wrapped);
			w.decreaseIndent();
			w.append(")");
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

		private ExpressionBuilder<P> isNullable(RelationalOperator _operator) {
			final ExpressionBuilder<P> rhs = new ExpressionBuilder<>(parent);
			currentCondition = new SimpleConditionBuilder(lhs, _operator, rhs, true);
			return rhs;
		}

		// @formatter:off
		/**
		 * Build an '=' expression. 
		 */
		public ExpressionBuilder<P> eq() { return is(RelationalOperator.EQ); }
		/**
		 * Build a '<>' expression. 
		 */
		public ExpressionBuilder<P> notEq() { return is(RelationalOperator.NOT_EQ); }
		/**
		 * Build an '=' expression. If the right-hand side represents the {@code NULL} value, then the operator is automatically converted to {@code IS}. 
		 */
		public ExpressionBuilder<P> eqNullable() { return isNullable(RelationalOperator.EQ); }
		/**
		 * Build a '<>' expression. If the right-hand side represents the {@code NULL} value, then the operator is automatically converted to {@code IS NOT}. 
		 */
		public ExpressionBuilder<P> notEqNullable() { return isNullable(RelationalOperator.NOT_EQ); }
		/**
		 * Build a '>' expression. 
		 */
		public ExpressionBuilder<P> gt() { return is(RelationalOperator.GT); }
		/**
		 * Build a '<' expression. 
		 */
		public ExpressionBuilder<P> lt() { return is(RelationalOperator.LT); }
		/**
		 * Build a '>=' expression. 
		 */
		public ExpressionBuilder<P> gte() { return is(RelationalOperator.GTE); }
		/**
		 * Build a '<=' expression. 
		 */
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
			currentCondition = new SimpleConditionBuilder(lhs, RelationalOperator.IS, SqlFragment.nullLiteral);
			return parent;
		}

		public P isNotNull() {
			currentCondition = new SimpleConditionBuilder(lhs, RelationalOperator.IS_NOT, SqlFragment.nullLiteral);
			return parent;
		}

		public P like(String text, char escapeChar) {
			// TODO use placeholder instead
			currentCondition = new SimpleConditionBuilder(lhs, RelationalOperator.LIKE,
					new SqlRaw(SqlUtils.escapeLikeString(text, escapeChar) + "'"));
			return parent;
		}

		public P like(String text) {
			// TODO use placeholder instead
			currentCondition = new SimpleConditionBuilder(lhs, RelationalOperator.LIKE,
					new SqlRaw(SqlUtils.toLiteralString(text)));
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
		INNER("inner join"), LEFT("left join"), RIGHT("right join"), FULL("full join"), CROSS("cross join");

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
		public StatementOperation toStatement() {
			return new StatementOperationImpl(cnxProvider, getSql(), this);
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
			final CompositeIterator<SqlFragment> fragmentsIterator = new CompositeIterator<>(
					Arrays.asList(whereClauses.iterator()));
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
			return new Query<>(cnxProvider, getSql(), this, extractor);
		}
	}

	public enum Placement {
		BEFORE_KEYWORD, AFTER_KEYWORD, AFTER_EXPRESSION
	}

	public enum SqlSelectClause implements SqlFragment {
		WITH("with"),
		SELECT("select"),
		FROM("from"),
		WHERE("where"),
		GROUP_BY("group by"),
		HAVING("having"),
		ORDER_BY("order by"),
		OFFSET("offset"),
		FETCH_FIRST("fetch first");

		private final String sql;

		SqlSelectClause(String sql) {
			this.sql = sql;
		}

		@Override
		public String getSql() {
			return sql;
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			w.append(sql);
		}
	}

	/**
	 * Store additional raw clauses used in SELECT statements.
	 */
	private static class SqlRawMap {
		private final List<List<SqlRaw>> rawClauses;
		private static final int totalLocationCount = SqlSelectClause.values().length;

		public SqlRawMap() {
			final int keyCount = Placement.values().length * SqlSelectClause.values().length;
			rawClauses = new ArrayList<>(keyCount);
			for (int i = 0; i < keyCount; i++) {
				rawClauses.add(null);
			}
		}

		private int getIndex(Placement placement, SqlSelectClause location) {
			return placement.ordinal() * totalLocationCount + location.ordinal();
		}

		public List<SqlRaw> get(Placement placement, SqlSelectClause location) {
			final int index = getIndex(placement, location);
			final List<SqlRaw> clauses = rawClauses.get(index);
			return clauses == null ? Collections.emptyList() : clauses;
		}

		public void add(Placement placement, SqlSelectClause location, SqlRaw clause) {
			final int index = getIndex(placement, location);
			List<SqlRaw> clauses = rawClauses.get(index);
			if (clauses == null) {
				clauses = new ArrayList<>(1);
				rawClauses.set(index, clauses);
			}
			clauses.add(clause);
		}
	}

	public class SqlSelectBuilder extends SqlSelectStatement {
		private final Collection<SqlFragment> withClauses = new ArrayList<>();
		private final Collection<SqlFragment> selectClauses = new ArrayList<>();
		private SqlFragment fromClause;
		private final Collection<String> joinClauses = new ArrayList<>();
		private final Collection<SqlFragment> whereClauses = new ArrayList<>();
		private final Collection<SqlFragment> havingClauses = new ArrayList<>();
		private final Collection<SqlFragment> groupByClauses = new ArrayList<>();
		private final Collection<SqlFragment> orderByClauses = new ArrayList<>();
		private SqlFragment offsetClause;
		private SqlFragment fetchFirstClause;
		private final SqlRawMap additionalClauses = new SqlRawMap();

		public SqlSelectBuilder distinct() {
			additionalClauses.add(Placement.AFTER_KEYWORD, SqlSelectClause.SELECT, new SqlRaw("distinct"));
			return this;
		}

		public WithClauseBuilder with(String pseudoTableName) {
			final WithClauseBuilder res = new WithClauseBuilder(pseudoTableName, this);
			withClauses.add(res);
			return res;
		}

		/**
		 * Add a SELECT clause having the following form:
		 * <p>
		 * {@code SELECT 'literal' as columnAlias}
		 */
		public SqlSelectBuilder selectLiteral(String literal, String columnAlias) {
			final StringBuilder clause_str = new StringBuilder(SqlUtils.toLiteralString(literal));
			if (columnAlias != null) clause_str.append(" AS " + columnAlias);
			selectClauses.add(new SqlRaw(clause_str.toString()));

			return this;
		}

		public SqlSelectBuilder select(String _selectClause) {
			selectClauses.add(new SqlRaw(_selectClause));
			return this;
		}

		public SqlSelectBuilder select(String... _selects) {
			return select(Arrays.asList(_selects));
		}

		public SqlSelectBuilder select(Collection<String> _selects) {
			this.selectClauses.addAll(_selects.stream().map(SqlRaw::new).collect(Collectors.toList()));
			return this;
		}

		public SqlSelectBuilder from(String _fromClause) {
			if (fromClause != null) throw new IllegalStateException();
			this.fromClause = new SqlRaw(_fromClause);
			return this;
		}

		public SqlSelectBuilder from(SqlSelectBuilder _fromClause) {
			if (fromClause != null) throw new IllegalStateException("from clause has already been set");
			this.fromClause = SqlFragment.wrapInParentheses(_fromClause, true);
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

		public SqlSelectBuilder crossJoin(String joinClause) {
			return join(JoinType.CROSS, joinClause);
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
			for (final String f : _groupByClauses) {
				groupByClauses.add(new SqlRaw(f));
			}
			return this;
		}

		public SqlSelectBuilder orderBy(String... _orderByClauses) {
			for (final String f : _orderByClauses) {
				orderByClauses.add(new SqlRaw(f));
			}
			return this;
		}

		/**
		 * Row offset. {@code 0} means no offset.<br>
		 * Introduced in the SQL:2008 standard.
		 */
		public SqlSelectBuilder offset(int offset) {
			if (offsetClause != null) throw new IllegalStateException("offset clause has already been set");
			offsetClause = new SqlRaw("? rows", (ps, index) -> ps.setInt(index.next(), offset));
			return this;
		}

		public SqlSelectBuilder fetchFirst(int rowCount) {
			if (fetchFirstClause != null) throw new IllegalStateException("fetch first clause has already been set");
			fetchFirstClause = new SqlRaw("? rows only", (ps, index) -> ps.setInt(index.next(), rowCount));
			return this;
		}

		public SqlSelectBuilder raw(Placement placement, SqlSelectClause location, String _sql,
				PreparedStatementBinder binder) {
			final SqlRaw clause = new SqlRaw(_sql, binder);
			additionalClauses.add(placement, location, clause);
			return this;
		}

		public SqlSelectBuilder raw(Placement placement, SqlSelectClause location, String _sql) {
			return raw(placement, location, _sql, null);
		}

		/**
		 * @param w
		 * @param clause The type of SELECT clause.
		 * @param fragments The SQL fragments making the SQL clause.
		 * @param newline If
		 *        {@code true, separate fragments with newline and increment indentation of fragments (except when there is only one fragment).
		 * 		@param joinString The join string between fragments.
		 */
		private void writeClause(SqlStringBuilder w, SqlSelectClause clause,
				Collection<? extends SqlFragment> fragments, boolean newline, String joinString) {
			additionalClauses.get(Placement.BEFORE_KEYWORD, clause).forEach(w::appendln);
			if (!fragments.isEmpty()) {
				w.append(clause);
				additionalClauses.get(Placement.AFTER_KEYWORD, clause).forEach(i -> w.append(" ").append(i));
				if (newline && fragments.size() > 1) {
					w.appendln();
					w.increaseIndent();
				} else {
					w.append(" ");
				}
				forEach_endAware(fragments, (fragment, first, last) -> {
					if (newline) w.appendln(fragment);
					else w.append(fragment);
					if (!last) w.append(joinString);
				});
				if (newline) {
					if (fragments.size() > 1) w.decreaseIndent();
				} else {
					w.appendln();
				}
			} else {
				additionalClauses.get(Placement.AFTER_KEYWORD, clause).forEach(w::appendln);
			}
			additionalClauses.get(Placement.AFTER_EXPRESSION, clause).forEach(w::appendln);
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			writeClause(w, SqlSelectClause.WITH, withClauses, true, ",");
			writeClause(w, SqlSelectClause.SELECT, selectClauses, false, ", ");

			// begin - special case for "from" clause
			additionalClauses.get(Placement.BEFORE_KEYWORD, SqlSelectClause.FROM).forEach(w::appendln);
			w.append(SqlSelectClause.FROM).append(" ");
			additionalClauses.get(Placement.AFTER_KEYWORD, SqlSelectClause.FROM)
					.forEach(i -> w.append(i).append(" "));
			w.appendln(fromClause);
			joinClauses.forEach(w::appendln);
			additionalClauses.get(Placement.AFTER_EXPRESSION, SqlSelectClause.FROM).forEach(w::appendln);
			// end - special case for "from" clause

			writeClause(w, SqlSelectClause.WHERE, whereClauses, true, "and ");
			writeClause(w, SqlSelectClause.GROUP_BY, groupByClauses, false, ", ");
			writeClause(w, SqlSelectClause.HAVING, havingClauses, true, "and ");
			writeClause(w, SqlSelectClause.ORDER_BY, orderByClauses, false, ", ");
			writeClause(w, SqlSelectClause.OFFSET, getOffsetClauses(), false, "");
			writeClause(w, SqlSelectClause.FETCH_FIRST, getFetchFirstClauses(), false, "");
		}

		public Collection<SqlFragment> getOffsetClauses() {
			return offsetClause == null ? Collections.emptyList() : Collections.singleton(offsetClause);
		}

		public Collection<SqlFragment> getFetchFirstClauses() {
			return fetchFirstClause == null ? Collections.emptyList() : Collections.singleton(fetchFirstClause);
		}

		public Collection<SqlFragment> getFromClauses() {
			return Collections.singleton(fromClause);
		}

		@Override
		public void bind(PreparedStatement ps, IntSequence index) throws SQLException {
			// @formatter:off
			final CompositeIterator<SqlFragment> fragmentsIterator = new CompositeIterator<>(Arrays.asList(
				withClauses.iterator(),
				selectClauses.iterator(),
				getFromClauses().iterator(),
				whereClauses.iterator(),
				groupByClauses.iterator(),
				havingClauses.iterator(),
				orderByClauses.iterator(),
				getOffsetClauses().iterator(),
				getFetchFirstClauses().iterator()
			));
			// @formatter:on
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
			w.append(setClauses.stream().map(SetValueClause::getValue).map(SqlFragment::getSql)
					.collect(Collectors.joining(", ")));
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

		/**
		 * Convience method. Equivalent to: {@code values().set(columnName)}.
		 */
		public ExpressionBuilder<InsertValuesBuilder> set(String columnName) {
			return values().set(columnName);
		}

		@Override
		public void bind(PreparedStatement ps, IntSequence index) throws SQLException {
			if (body != null) body.bind(ps, index);
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			w.append("insert into ").append(tableName);
			if (columns != null) {
				w.append(" (");
				w.append(columns.stream().collect(Collectors.joining(", ")));
				w.appendln(")");
			}
			if (body != null) {
				w.append(body);
			} else {
				// FIXME not accepted by all dialects
				w.appendln();
				w.append("default values");
			}
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
			final CompositeIterator<SqlFragment> fragmentsIterator = new CompositeIterator<>(
					Arrays.asList(setClauses.iterator(), whereClauses.iterator()));
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
			setClauses.add(new SqlMergeClause(
					EnumSet.of(SqlMergeClauseFlag.ON_CLAUSE, SqlMergeClauseFlag.INSERT_CLAUSE), columnName, res));
			return res;
		}

		public ExpressionBuilder<SqlMergeBuilder> insertOrUpdate(String columnName) {
			final ExpressionBuilder<SqlMergeBuilder> res = new ExpressionBuilder<>(this);
			setClauses.add(new SqlMergeClause(
					EnumSet.of(SqlMergeClauseFlag.UPDATE_CLAUSE, SqlMergeClauseFlag.INSERT_CLAUSE), columnName, res));
			return res;
		}

		public ExpressionBuilder<SqlMergeBuilder> insert(String columnName) {
			final ExpressionBuilder<SqlMergeBuilder> res = new ExpressionBuilder<>(this);
			setClauses.add(new SqlMergeClause(EnumSet.of(SqlMergeClauseFlag.INSERT_CLAUSE), columnName, res));
			return res;
		}

		private List<SqlMergeClause> getInsertClauses() {
			return setClauses.stream().filter(c -> c.getFlags().contains(SqlMergeClauseFlag.INSERT_CLAUSE))
					.collect(Collectors.toList());
		}

		private List<SqlMergeClause> getUpdateClauses() {
			return setClauses.stream().filter(c -> c.getFlags().contains(SqlMergeClauseFlag.UPDATE_CLAUSE))
					.collect(Collectors.toList());
		}

		private Condition getOnCondition() {
			final List<Condition> onClauses = setClauses.stream()
					.filter(c -> c.getFlags().contains(SqlMergeClauseFlag.ON_CLAUSE))
					.map(c -> new SimpleConditionBuilder(new SqlRaw(c.getColumnName()), RelationalOperator.EQ,
							c.getValue(), true))
					.collect(Collectors.toList());
			return new CompositeConditionBuilder(onClauses, LogicalOperator.AND);
		}

		@Override
		public void bind(PreparedStatement ps, IntSequence index) throws SQLException {
			final Condition onCondition = getOnCondition();
			final List<SqlMergeClause> updateClauses = getUpdateClauses();
			final List<SqlMergeClause> insertClauses = getInsertClauses();

			onCondition.bind(ps, index);
			for (final SqlMergeClause c : updateClauses) {
				c.bind(ps, index);
			}
			for (final SqlMergeClause c : insertClauses) {
				c.bind(ps, index);
			}
		}

		@Override
		public void appendTo(SqlStringBuilder w) {
			final Condition onCondition = getOnCondition();
			final List<SqlMergeClause> updateClauses = getUpdateClauses();
			final List<SqlMergeClause> insertClauses = getInsertClauses();

			w.append("merge into ").append(tableName).appendln(" using dual on (");
			w.increaseIndent();
			w.appendln(onCondition);
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
			w.append(insertClauses.stream().map(SqlMergeClause::getValue).map(SqlFragment::getSql)
					.collect(Collectors.joining(", ")));
			w.append(")");
		}
	}

	@FunctionalInterface
	private interface EndAwareConsumer<T> {
		void accept(T value, boolean first, boolean last);
	}

	static <T> void forEach_endAware(Collection<? extends T> collection, EndAwareConsumer<T> consumer) {
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
	 * Represent a batch statement. Unlike in JDBC where a batch statement is represented by a single
	 * {@link java.sql.Statement} object, here the BatchStatement holds a collection of {@link SqlStatement} items.
	 * <p>
	 * Warning: each {@link SqlStatement} item must represent the same SQL statement. Only the first statement will be
	 * converted to SQL.
	 * <p>
	 * At the moment, there is no check to actually make sure the SQL is the same. This may change in the future.
	 */
	public class BatchStatementBuilder {
		private final Collection<SqlStatement> statements;
		private long executeEveryNRow = -1;
		private long commitEveryNRow = -1;

		public BatchStatementBuilder() {
			this.statements = new ArrayList<>();
		}

		public BatchStatementBuilder executeEveryNRow(int _executeEveryNRow) {
			this.executeEveryNRow = _executeEveryNRow;
			return this;
		}

		public BatchStatementBuilder commitEveryNRow(int _commitEveryNRow) {
			this.commitEveryNRow = _commitEveryNRow;
			return this;
		}

		public BatchStatementBuilder(Collection<? extends SqlStatement> statements) {
			this.statements = new ArrayList<>(statements);
		}

		public void add(SqlStatement statement) {
			statements.add(statement);
		}

		public void addAll(Collection<? extends SqlStatement> statements) {
			this.statements.addAll(statements);
		}

		public BatchStatementOperation<SqlStatement> toStatement() {
			return new BatchStatementOperation<>(cnxProvider, statements.stream(), executeEveryNRow,
					commitEveryNRow);
		}
	}

	/**
	 * Debug statements by printing the value of prepared values in a comment next to the '?' placeholder.
	 */
	public boolean isDebug() {
		return debug;
	}

	public ConnectionProvider getConnectionProvider() {
		return cnxProvider;
	}
}
