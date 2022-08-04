//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

extension Connection {
	/// Available per-connection database limit parameters.
	///
	/// - seealso: [Run-Time Limit Categories](https://www.sqlite.org/c3ref/c_limit_attached.html)
	public enum LimitParameter {
		/// The maximum size of any string or BLOB or table row, in bytes.
		case length
		/// The maximum number of columns in a table definition or in the result set of a `SELECT` or the maximum number of columns in an index or in an `ORDER BY` or `GROUP BY` clause.
		case sqlLength
		/// The approximate number of bytes of heap memory used to store the schema for all databases.
		case column
		/// The maximum depth of the parse tree on any expression.
		case exprDepth
		/// The maximum number of terms in a compound SELECT statement.
		case compoundSelect
		/// The maximum number of instructions in a virtual machine program used to implement an SQL statement.
		/// If `sqlite3_prepare_v2()` or the equivalent tries to allocate space for more than this many opcodes in a single prepared statement, an `SQLITE_NOMEM` error is returned.
		case vdbeOp
		/// The maximum number of arguments on a function.
		case functionArg
		/// The maximum number of attached databases.
		case attached
		/// The maximum length of the pattern argument to the LIKE or GLOB operators.
		case likePatternLength
		/// The maximum index number of any parameter in an SQL statement.
		case variableNumber
		/// The maximum depth of recursion for triggers.
		case triggerDepth
		/// The maximum number of auxiliary worker threads that a single prepared statement may start.
		case workerThreads
	}

	/// Sets a new limit for `parameter` and returns the previous limit.
	///
	/// - parameter parameter: The desired database parameter.
	/// - parameter value: The new limit value.
	///
	/// - returns: The previous value of the limit.
	///
	/// - note: To query the current value of a limt, pass `-1` for `value`.
	///
	/// - seealso: [Run-time Limits](https://www.sqlite.org/c3ref/limit.html)
	public func limit(ofParameter parameter: LimitParameter, value: Int = -1) -> Int {
		let op: Int32
		switch parameter {
		case .length:
			op = SQLITE_LIMIT_LENGTH
		case .sqlLength:
			op = SQLITE_LIMIT_SQL_LENGTH
		case .column:
			op = SQLITE_LIMIT_COLUMN
		case .exprDepth:
			op = SQLITE_LIMIT_EXPR_DEPTH
		case .compoundSelect:
			op = SQLITE_LIMIT_COMPOUND_SELECT
		case .vdbeOp:
			op = SQLITE_LIMIT_VDBE_OP
		case .functionArg:
			op = SQLITE_LIMIT_FUNCTION_ARG
		case .attached:
			op = SQLITE_LIMIT_ATTACHED
		case .likePatternLength:
			op = SQLITE_LIMIT_LIKE_PATTERN_LENGTH
		case .variableNumber:
			op = SQLITE_LIMIT_VARIABLE_NUMBER
		case .triggerDepth:
			op = SQLITE_LIMIT_TRIGGER_DEPTH
		case .workerThreads:
			op = SQLITE_LIMIT_WORKER_THREADS
		}

		return Int(sqlite3_limit(databaseConnection, op, Int32(value)))
	}
}
