//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

/// An `sqlite3_stmt *` object.
///
/// - seealso: [SQLite Prepared Statement Object](https://sqlite.org/c3ref/stmt.html)
public typealias SQLitePreparedStatement = OpaquePointer

/// A compiled SQL statement with support for SQL parameter binding and result row processing.
///
/// **Creation**
///
/// A statement is not created directly but is obtained from a `Database`.
///
/// ```swift
/// let statement = try database.prepare(sql: "select count(*) from t1;")
/// ```
///
/// **Parameter Binding**
///
/// A statement supports binding values to SQL parameters by index or by name.
///
/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
///
/// ```swift
/// let statement = try database.prepare(sql: "insert into t1(a, b, c, d, e, f) values (?, ?, ?, :d, :e, :f);")
/// try statement.bind(integer: 30, toParameter: 3)
/// try statement.bind(integer: 40, toParameter: ":d")
/// try statement.bind(parameterValues: 10, 20)
/// try statement.bind(parameters: [":f": 60, ":e": 50])
/// ```
///
/// **Result Rows**
///
/// When executed a statement provides zero or more result rows.
///
/// ```swift
/// try statement.results { row in
///     // Do something with `row`
/// }
/// ```
///
/// ```swift
/// for row in statement {
///     // Do something with `row`
/// }
/// ```
///
/// It is generally preferred to use the block-based method because any errors may be explicitly handled instead of
/// silently discarded.
public final class Statement {
	/// The owning database.
	public let database: Database
	/// The underlying `sqlite3_stmt *` object.
	let preparedStatement: SQLitePreparedStatement

	/// Creates a compiled SQL statement.
	///
	/// - attention: The statement takes ownership of `preparedStatement`.  The result of further use of `preparedStatement` is undefined.
	///
	/// - parameter database: The owning database.
	/// - parameter preparedStatement: An `sqlite3_stmt *` prepared statement object..
	///
	/// - throws: An error if `sql` could not be compiled.
	public init(database: Database, preparedStatement: SQLitePreparedStatement) {
		precondition(sqlite3_db_handle(preparedStatement) == database.databaseConnection)
		self.database = database
		self.preparedStatement = preparedStatement
	}

	deinit {
		_ = sqlite3_finalize(preparedStatement)
	}

	/// Creates a compiled SQL statement.
	///
	/// - parameter database: The owning database.
	/// - parameter sql: The SQL statement to compile.
	///
	/// - throws: An error if `sql` could not be compiled.
	public convenience init(database: Database, sql: String) throws {
		var stmt: SQLitePreparedStatement?
		guard sqlite3_prepare_v2(database.databaseConnection, sql, -1, &stmt, nil) == SQLITE_OK else {
			throw SQLiteError("Error preparing SQL \"\(sql)\"", takingErrorCodeFromDatabaseConnection: database.databaseConnection)
		}
		precondition(stmt != nil)
		self.init(database: database, preparedStatement: stmt.unsafelyUnwrapped)
	}

	/// `true` if this statement makes no direct changes to the database, `false` otherwise.
	///
	/// - seealso: [Read-only statements in SQLite](https://sqlite.org/c3ref/stmt_readonly.html)
	public var isReadOnly: Bool {
		sqlite3_stmt_readonly(preparedStatement) != 0
	}

	/// The number of columns in the result set.
	public var columnCount: Int {
		Int(sqlite3_column_count(preparedStatement))
	}

	/// The names of the columns.
	///
	/// - note: Column names are not guaranteed to be unique.
	public lazy var columnNames: [String] = {
		let count = sqlite3_column_count(preparedStatement)
		var names: [String] = []
		for i in 0 ..< count {
			if let s = sqlite3_column_name(preparedStatement, i) {
				names.append(String(cString: s))
			} else {
				names.append("")
			}
		}
		return names
	}()

	/// Returns the name of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a result row has index 0.
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds or an out-of-memory error occurs.
	///
	/// - returns: The name of the column for the specified index.
	public func nameOfColumn(_ index: Int) throws -> String {
		guard let name = sqlite3_column_name(preparedStatement, Int32(index)) else {
			throw DatabaseError("Column index \(index) out of bounds")
		}
		return String(cString: name)
	}

	/// The mapping of column names to indexes.
	lazy var columnNamesAndIndexes: [String: Int] = {
		let count = sqlite3_column_count(preparedStatement)
		var map = [String: Int](minimumCapacity: Int(count))
		for i in 0 ..< count {
			if let s = sqlite3_column_name(preparedStatement, i) {
				map[String(cString: s)] = Int(i)
			}
		}
		return map
	}()

	/// Returns the index of a column with `name`.
	///
	/// - note: Column names are not guaranteed to be unique.
	///
	/// - parameter name: The name of the desired column.
	///
	/// - throws: An error if the column doesn't exist.
	///
	/// - returns: The index of a column with the specified name.
	public func indexOfColumn(_ name: String) throws -> Int {
		guard let index = columnNamesAndIndexes[name] else {
			throw DatabaseError("Unknown column \"\(name)\"")
		}
		return index
	}
}

extension Statement {
	/// Performs a low-level SQLite statement operation.
	///
	/// - attention: **Use of this function should be avoided whenever possible.**
	///
	/// - parameter block: A closure performing the statement operation.
	/// - parameter preparedStatement: The raw `sqlite3_stmt *` prepared statement object.
	///
	/// - throws: Any error thrown in `block`.
	///
	/// - returns: The value returned by `block`.
	public func withUnsafeSQLitePreparedStatement<T>(block: (_ preparedStatement: SQLitePreparedStatement) throws -> (T)) rethrows -> T {
		try block(preparedStatement)
	}
}

extension Statement {
	/// Executes the statement and discards any result rows.
	///
	/// - throws: An error if the statement could not be executed.
	public func execute() throws {
		var result = sqlite3_step(preparedStatement)
		while result == SQLITE_ROW {
			result = sqlite3_step(preparedStatement)
		}
		guard result == SQLITE_DONE else {
			throw SQLiteError("Error evaluating statement", takingErrorCodeFromDatabaseConnection: database.databaseConnection)
		}
	}

	/// Executes the statement and applies `block` to each result row.
	///
	/// - parameter block: A closure applied to each result row.
	/// - parameter row: A result row of returned data.
	///
	/// - throws: Any error thrown in `block` or an error if the statement did not successfully run to completion.
	public func results(_ block: ((_ row: Row) throws -> ())) throws {
		var result = sqlite3_step(preparedStatement)
		while result == SQLITE_ROW {
			try block(Row(statement: self))
			result = sqlite3_step(preparedStatement)
		}
		guard result == SQLITE_DONE else {
			throw SQLiteError("Error evaluating statement", takingErrorCodeFromDatabaseConnection: database.databaseConnection)
		}
	}

	/// Returns the next result row or `nil` if none.
	///
	/// - returns: The next result row of returned data.
	///
	/// - throws: An error if the statement encountered an execution error.
	public func step() throws -> Row? {
		switch sqlite3_step(preparedStatement) {
		case SQLITE_ROW:
			return Row(statement: self)
		case SQLITE_DONE:
			return nil
		default:
			throw SQLiteError("Error evaluating statement", takingErrorCodeFromDatabaseConnection: database.databaseConnection)
		}
	}

	/// Resets the statement to its initial state, ready to be re-executed.
	///
	/// - note: This function does not change the value of  any bound SQL parameters.
	///
	/// - throws: An error if the statement could not be reset.
	public func reset() throws {
		guard sqlite3_reset(preparedStatement) == SQLITE_OK else {
			throw SQLiteError("Error resetting statement", takingErrorCodeFromDatabaseConnection: database.databaseConnection)
		}
	}
}

extension Statement {
	/// The original SQL text of the statement.
	public var sql: String {
		guard let str = sqlite3_sql(preparedStatement) else {
			return ""
		}
		return String(cString: str)
	}

#if SQLITE_ENABLE_NORMALIZE
	/// The normalized SQL text of the statement.
	public var normalizedSQL: String {
		guard let str = sqlite3_normalized_sql(preparedStatement) else {
			return ""
		}
		return String(cString: str)
	}
#endif

	/// The SQL text of the statement with bound parameters expanded.
	public var expandedSQL: String {
		guard let str = sqlite3_expanded_sql(preparedStatement) else {
			return ""
		}
		defer {
			sqlite3_free(str)
		}
		return String(cString: str)
	}
}

extension Statement: Sequence {
	/// Returns an iterator for accessing the result rows.
	///
	/// Because the iterator discards errors, the preferred way of accessing result rows
	/// is via `nextRow()` or `results(_:)`.
	///
	/// - returns: An iterator over the result rows.
	public func makeIterator() -> Statement {
		self
	}
}

extension Statement: IteratorProtocol {
	/// Returns the next result row or `nil` if none.
	///
	/// Because the iterator discards errors, the preferred way of accessing result rows
	/// is via `nextRow()` or `results(_:)`.
	///
	/// - returns: The next result row of returned data.
	public func next() -> Row? {
		try? step()
	}
}

extension Statement {
	/// Returns the value of the column at `index` for each row in the result set.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	///
	/// - requires: `index >= 0`.
	/// - requires: `index < self.columnCount`.
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds.
	public func column(_ index: Int) throws -> [DatabaseValue] {
		var values = [DatabaseValue]()
		try results { row in
			values.append(try row.value(at: index))
		}
		return values
	}

	/// Returns the value of the column with `name` for each row in the result set.
	///
	/// - parameter name: The name of the desired column.
	///
	/// - throws: An error if the column `name` doesn't exist.
	public func column(_ name: String) throws -> [DatabaseValue] {
		let index = try indexOfColumn(name)
		var values = [DatabaseValue]()
		try results { row in
			values.append(try row.value(at: index))
		}
		return values
	}

	/// Returns the values of the columns at `indexes` for each row in the result set.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	///
	/// - requires: `indexes.min() >= 0`.
	/// - requires: `indexes.max() < self.columnCount`.
	///
	/// - parameter indexes: The indexes of the desired columns.
	///
	/// - throws: An error if any element of `indexes` is out of bounds.
	public func columns<S: Collection>(_ indexes: S) throws -> [[DatabaseValue]] where S.Element == Int {
		var values = [[DatabaseValue]](repeating: [], count: indexes.count)
		for (n, x) in indexes.enumerated() {
			values[n] = try self.column(x)
		}
		return values
	}

	/// Returns the values of the columns with `names` for each row in the result set.
	///
	/// - parameter names: The names of the desired columns.
	///
	/// - throws: An error if a column in `names` doesn't exist.
	public func columns<S: Collection>(_ names: S) throws -> [String: [DatabaseValue]] where S.Element == String {
		var values: [String: [DatabaseValue]] = [:]
		for name in names {
			values[name] = try self.column(name)
		}
		return values
	}
}
