package com.github.fjdbc.codegen;

import java.io.IOException;
import java.io.Writer;

public class AbstractWriter implements AutoCloseable {
	protected final Writer writer;

	public AbstractWriter(Writer writer) {
		assert writer != null;
		this.writer = writer;

	}

	public void writeln() throws IOException {
		writeln("");
	}

	public void writeln(String format, Object... args) throws IOException {
		writer.write(String.format(format, args) + "\n");
	}

	@Override
	public void close() throws IOException {
		writer.close();
	}
}
