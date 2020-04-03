package com.github.fjdbc.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import com.github.fjdbc.ConnectionProvider;
import com.github.fjdbc.IntSequence;
import com.github.fjdbc.RuntimeSQLException;
import com.github.fjdbc.SQLConsumer;
import com.github.fjdbc.op.StatementOperation;
import com.github.fjdbc.sql.SqlBuilder.SqlFragment;

/**
 * A {@code StatementOperation} that executes as a batch statement.
 * @param <T>
 */
public class BatchStatementOperation<T extends SqlFragment> implements StatementOperation {
	private SQLConsumer<Statement> beforeExecutionConsumer;
	private SQLConsumer<Statement> afterExecutionConsumer;
	private final Stream<T> statements;
	private final ConnectionProvider cnxProvider;
	private final long executeEveryNRow;
	private final long commitEveryNRow;
	private BiConsumer<SQLException, T> errorHandler = (e, statement) -> {
		throw new RuntimeSQLException(e);
	};
	private AtomicBoolean cancelRequested = new AtomicBoolean();

	public BatchStatementOperation(ConnectionProvider cnxProvider, Stream<T> statements, long executeEveryNRow,
			long commitEveryNRow) {
		this.cnxProvider = cnxProvider;
		this.statements = statements;
		this.executeEveryNRow = executeEveryNRow;
		this.commitEveryNRow = commitEveryNRow;
	}

	@Override
	public StatementOperation doBeforeExecution(SQLConsumer<Statement> beforeExecutionConsumer) {
		this.beforeExecutionConsumer = beforeExecutionConsumer;
		return this;
	}

	@Override
	public StatementOperation doAfterExecution(SQLConsumer<Statement> afterExecutionConsumer) {
		this.afterExecutionConsumer = afterExecutionConsumer;
		return this;
	}

	@Override
	public int execute(Connection cnx) throws SQLException {
		assert cnx != null;
		return execute_preparedStatement(cnx);
	}

	private int execute_preparedStatement(Connection cnx) throws SQLException {
		// since we need to access local variables inside the Consumer, we wrap each variables in an array of
		// size 1
		final PreparedStatement[] ps = new PreparedStatement[1];
		final String[] sql = new String[1];
		final int[] nRows = new int[1];
		nRows[0] = 0;
		final IntSequence count = new IntSequence(0);

		final SQLConsumer<T> consumer = st -> {
			if (ps[0] == null) {
				sql[0] = st.getSql();
				ps[0] = cnx.prepareStatement(sql[0]);
			}
			beforeExecution(ps[0]);
			try {
				st.bind(ps[0], new IntSequence(1));
				ps[0].addBatch();
				count.next();
				if (cancelRequested.get()) {
					cnxProvider.rollback(cnx);
					throw new CancellationException();
				}
				if (executeEveryNRow > 0 && (count.get() % executeEveryNRow) == 0) {
					final int[] nRows_array = ps[0].executeBatch();
					nRows[0] += getNRowsModifiedByBatch(nRows_array);
				}
				if (commitEveryNRow > 0 && (count.get() % commitEveryNRow) == 0) {
					cnxProvider.commit(cnx);
				}
			} catch (final SQLException e) {
				errorHandler.accept(e, st);
			}

			final int[] nRows_array = ps[0].executeBatch();
			nRows[0] += getNRowsModifiedByBatch(nRows_array);
			afterExecution(ps[0]);
		};

		try {
			statements.forEachOrdered(consumer.uncheck());
		} catch (final Exception e) {
			if (!(e instanceof CancellationException)) {
				throw e;
			}
		} finally {
			close(ps[0]);
			statements.close();
		}
		return nRows[0];
	}

	private void beforeExecution(final PreparedStatement ps) throws SQLException {
		if (beforeExecutionConsumer != null) beforeExecutionConsumer.accept(ps);
	}

	private void afterExecution(final PreparedStatement ps) throws SQLException {
		if (afterExecutionConsumer != null) afterExecutionConsumer.accept(ps);
	}

	private static void close(Statement st) {
		try {
			if (st != null) st.close();
		} catch (final SQLException e) {
			throw new RuntimeSQLException(e);
		}
	}

	private int getNRowsModifiedByBatch(int[] modifiedRows) {
		int sum = 0;
		for (final int r : modifiedRows) {
			if (r == Statement.SUCCESS_NO_INFO) {
				return Statement.SUCCESS_NO_INFO;
			} else if (r == Statement.EXECUTE_FAILED) {
				return Statement.EXECUTE_FAILED;
			} else {
				sum += r;
			}
		}
		return sum;
	}

	@Override
	public int executeAndCommit() {
		Connection cnx = null;
		try {
			cnx = cnxProvider.borrow();
			final int modifiedRows = execute(cnx);
			cnxProvider.commit(cnx);
			return modifiedRows;
		} catch (final SQLException e) {
			throw new RuntimeSQLException("Error executing the stream of SQL statements", e);
		} finally {
			// if the connection was already committed, roll back should be a no op.
			cnxProvider.rollback(cnx);
			cnxProvider.giveBack(cnx);
		}
	}

	public void setErrorHandler(BiConsumer<SQLException, T> errorHandler) {
		this.errorHandler = errorHandler;
	}

	/**
	 * {@code cancelRequested} is an {@link AtomicBoolean} instance that allows to cancel the execution from a
	 * different thread.
	 * <p>
	 * If not set, an instance is provided by default.
	 * @param cancelRequested the {@link AtomicBoolean} reference.
	 */
	public void setCancelRequestAtomicBoolean(AtomicBoolean cancelRequested) {
		this.cancelRequested = cancelRequested;
	}

	/**
	 * Request the cancellation of this statement.
	 * This method is thread-safe.
	 */
	public void cancel() {
		cancelRequested.set(true);
	}

}