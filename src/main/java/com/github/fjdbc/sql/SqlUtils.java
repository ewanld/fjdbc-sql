package com.github.fjdbc.sql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SqlUtils {
	/**
	 * Partition a list into sublists of length L. The last list may have a size
	 * smaller than L.<br>
	 * The sublists are backed by the original list.
	 */
	public static <T> List<List<T>> partition(Collection<T> collection, final int partitionSize) {
		assert partitionSize > 0;
		assert collection != null;
		return partitionList(new ArrayList<T>(collection), partitionSize);
	}

	/**
	 * Partition a list into sublists of length L. The last list may have a size
	 * smaller than L.<br>
	 * The sublists are backed by the original list.
	 */
	private static <T> List<List<T>> partitionList(List<T> list, final int partitionSize) {
		assert partitionSize > 0;
		assert list != null;
		final List<List<T>> res = new ArrayList<List<T>>();
		for (int i = 0; i < list.size(); i += partitionSize) {
			res.add(list.subList(i, Math.min(list.size(), i + partitionSize)));
		}
		return res;
	}

	public static String escapeComment(String comment) {
		final String res = comment
				.replace("/*", "\\slash \\star")
				.replace("*/", "\\star \\slash")
				.replace("--", "\\minus \\minus");
		return res;
	}

	public static String escapeString(String text) {
		return text.replace("'", "''");
	}

	public static String escapeLikeString(String text, char escapeChar) {
		String res = escapeString(text);
		final String escapeString = String.valueOf(escapeChar);
		res = res.replace(escapeString, escapeString + escapeChar);
		res = res.replace("%", escapeString + "%");
		res = res.replace("_", escapeString + "_");
		return res;
	}

	public static String toLiteralString(String s) {
		return "'" + escapeString(s) + "'";
	}
}
