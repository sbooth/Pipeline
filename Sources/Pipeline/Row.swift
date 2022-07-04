//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

extension Database {
	/// A fundamental data type that may be stored in an SQLite database.
	///
	/// - seealso: [Datatypes In SQLite](https://sqlite.org/datatype3.html)
	public enum FundamentalType {
		/// An integer value.
		case integer
		/// A floating-point value.
		case real
		/// A text value.
		case text
		/// A blob (untyped bytes) value.
		case blob
		/// A null value.
		case null
	}
}

/// A result row containing one or more columns with type-safe value access.
///
/// **Creation**
///
/// A row is not created directly but is obtained from a `Statement`.
///
/// ```swift
/// try statement.execute() { row in
///     // Do something with `row`
/// }
/// ```
///
/// **Column Value Access**
///
/// The database-native column value is expressed by `DatabaseValue`, however custom type conversion is possible
/// using `ColumnValueConverter`.
///
/// The value of columns is accessed by the `value(at:)` or `value(named:)` methods.
///
/// ```swift
/// let value = try row.value(at: 0)
/// let uuid: UUID = try row.value(named: "session_uuid")
/// ```
///
/// It is also possible to iterate over column values:
///
/// ```swift
/// for row in statement {
///     for value in row {
///         // Do something with `value`
///     }
///
/// }
/// ```
///
/// This allows for simple result row processing at the expense of error handling.
public struct Row {
	/// The statement owning this row.
	public let statement: Statement
}

extension Row {
	/// The number of columns in the result row.
	///
	/// - seealso: [Number of columns in a result set](https://sqlite.org/c3ref/data_count.html)
	public var columnCount: Int {
		Int(sqlite3_data_count(statement.preparedStatement))
	}

	/// Returns the initial data type of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a result row has index 0.
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds.
	///
	/// - returns: The data type of the column.
	///
	/// - seealso: [Result values from a query](https://sqlite.org/c3ref/column_blob.html)
	public func type(ofColumn index: Int) throws -> Database.FundamentalType {
		let type = sqlite3_column_type(statement.preparedStatement, Int32(index))
		guard statement.database.success else {
			throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
		}
		switch type {
		case SQLITE_INTEGER:
			return .integer
		case SQLITE_FLOAT:
			return .real
		case SQLITE_TEXT:
			return .text
		case SQLITE_BLOB:
			return .blob
		case SQLITE_NULL:
			return .null
		default:
			fatalError("Unknown SQLite column type \(type) encountered for column \(index)")
		}
	}
}

extension Row {
	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds.
	///
	/// - returns: The column's value.
	public func value(at index: Int) throws -> DatabaseValue {
		let type = sqlite3_column_type(statement.preparedStatement, Int32(index))
		guard statement.database.success else {
			throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
		}
		switch type {
		case SQLITE_INTEGER:
			let i = sqlite3_column_int64(statement.preparedStatement, Int32(index))
			guard statement.database.success else {
				throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
			}
			return .integer(i)
		case SQLITE_FLOAT:
			let r = sqlite3_column_double(statement.preparedStatement, Int32(index))
			guard statement.database.success else {
				throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
			}
			return .real(r)
		case SQLITE_TEXT:
			let t = String(cString: sqlite3_column_text(statement.preparedStatement, Int32(index)))
			guard statement.database.success else {
				throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
			}
			return .text(t)
		case SQLITE_BLOB:
			guard let b = sqlite3_column_blob(statement.preparedStatement, Int32(index)) else {
				guard statement.database.success else {
					throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
				}
				return .blob(Data())
			}
			let count = Int(sqlite3_column_bytes(statement.preparedStatement, Int32(index)))
			let data = Data(bytes: b.assumingMemoryBound(to: UInt8.self), count: count)
			return .blob(data)
		case SQLITE_NULL:
			return .null
		default:
			fatalError("Unknown SQLite column type \(type) encountered for column \(index)")
		}
	}

	/// Returns the value of the column `name`.
	///
	/// - parameter name: The name of the desired column.
	///
	/// - throws: An error if the column`name` doesn't exist.
	///
	/// - returns: The column's value.
	public func value(named name: String) throws -> DatabaseValue {
		try value(at: statement.indexOfColumn(name))
	}
}

extension Row {
	/// Returns the values of all columns in the row.
	///
	/// - returns: An array of the row's values.
	public func values() throws -> [DatabaseValue] {
		var values: [DatabaseValue] = []
		for i in 0 ..< statement.columnCount {
			values.append(try value(at: i))
		}
		return values
	}

	/// Returns the names and values of all columns in the row.
	///
	/// - warning: This method will fail at runtime if the column names are not unique.
	///
	/// - returns: A dictionary of the row's values keyed by column name.
	public func valueDictionary() throws -> [String: DatabaseValue] {
		try Dictionary(uniqueKeysWithValues: statement.columnNames.enumerated().map({ ($0.element, try value(at: $0.offset)) }))
	}
}

extension Row {
	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	///
	/// - parameter index: The index of the desired column.
	///
	/// - returns: The column's value or `nil` if the column doesn't exist.
	public subscript(at index: Int) -> DatabaseValue? {
		try? value(at: index)
	}

	/// Returns the value of the column with `name`.
	///
	/// - parameter name: The name of the desired column.
	///
	/// - returns: The column's value or `nil` if the column doesn't exist.
	public subscript(named name: String) -> DatabaseValue? {
		try? value(named: name)
	}
}

extension Row: Collection {
	public var startIndex: Int {
		0
	}

	public var endIndex: Int {
		columnCount
	}

	public subscript(position: Int) -> DatabaseValue {
		do {
			return try value(at: position)
		} catch {
			return .null
		}
	}

	public func index(after i: Int) -> Int {
		i + 1
	}
}

// MARK: - Fundamental Type Access

extension Row {
	/// Returns the signed integer value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	/// - note: Automatic type conversion may be performed by SQLite depending on the column's initial data type.
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds.
	///
	/// - returns: The column's signed integer value.
	///
	/// - seealso: [Result values from a query](https://sqlite.org/c3ref/column_blob.html)
	public func integer(at index: Int) throws -> Int64 {
		let i = sqlite3_column_int64(statement.preparedStatement, Int32(index))
		guard statement.database.success else {
			throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
		}
		return i
	}

	/// Returns the floating-point value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	/// - note: Automatic type conversion may be performed by SQLite depending on the column's initial data type.
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds.
	///
	/// - returns: The column's floating-point value.
	///
	/// - seealso: [Result values from a query](https://sqlite.org/c3ref/column_blob.html)
	public func real(at index: Int) throws -> Double {
		let r = sqlite3_column_double(statement.preparedStatement, Int32(index))
		guard statement.database.success else {
			throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
		}
		return r
	}

	/// Returns the text value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	/// - note: Automatic type conversion may be performed by SQLite depending on the column's initial data type.
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds.
	///
	/// - returns: The column's text value.
	///
	/// - seealso: [Result values from a query](https://sqlite.org/c3ref/column_blob.html)
	public func text(at index: Int) throws -> String {
		let t = String(cString: sqlite3_column_text(statement.preparedStatement, Int32(index)))
		guard statement.database.success else {
			throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
		}
		return t
	}

	/// Returns the BLOB value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	/// - note: Automatic type conversion may be performed by SQLite depending on the column's initial data type.
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds.
	///
	/// - returns: The column's BLOB value.
	///
	/// - seealso: [Result values from a query](https://sqlite.org/c3ref/column_blob.html)
	public func blob(at index: Int) throws -> Data {
		guard let b = sqlite3_column_blob(statement.preparedStatement, Int32(index)) else {
			guard statement.database.success else {
				throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
			}
			return Data()
		}
		let count = Int(sqlite3_column_bytes(statement.preparedStatement, Int32(index)))
		return Data(bytes: b.assumingMemoryBound(to: UInt8.self), count: count)
	}
}

extension Row {
	/// Returns the signed integer value of the column with name `name`.
	///
	/// - parameter name: The name of the desired column.
	///
	/// - throws: An error if the column doesn't exist or contains an illegal value.
	///
	/// - returns: The column's signed integer value.
	///
	/// - seealso: [Result values from a query](https://sqlite.org/c3ref/column_blob.html)
	public func integer(named name: String) throws -> Int64 {
		return try integer(at: statement.indexOfColumn(name))
	}

	/// Returns the floating-point value of the column with name `name`.
	///
	/// - parameter name: The name of the desired column.
	///
	/// - throws: An error if the column doesn't exist or contains an illegal value.
	///
	/// - returns: The column's floating-point value.
	///
	/// - seealso: [Result values from a query](https://sqlite.org/c3ref/column_blob.html)
	public func real(named name: String) throws -> Double {
		return try real(at: statement.indexOfColumn(name))
	}

	/// Returns the text value of the column with name `name`.
	///
	/// - parameter name: The name of the desired column.
	///
	/// - throws: An error if the column doesn't exist or contains an illegal value.
	///
	/// - returns: The column's text value.
	///
	/// - seealso: [Result values from a query](https://sqlite.org/c3ref/column_blob.html)
	public func text(named name: String) throws -> String {
		return try text(at: statement.indexOfColumn(name))
	}

	/// Returns the BLOB value of the column with name `name`.
	///
	/// - parameter name: The name of the desired column.
	///
	/// - throws: An error if the column doesn't exist or contains an illegal value.
	///
	/// - returns: The column's BLOB value.
	///
	/// - seealso: [Result values from a query](https://sqlite.org/c3ref/column_blob.html)
	public func blob(named name: String) throws -> Data {
		return try blob(at: statement.indexOfColumn(name))
	}
}
