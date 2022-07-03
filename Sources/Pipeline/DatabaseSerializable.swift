//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

/// A type that may be serialized to and deserialized from a database.
///
/// This is a more general method for database storage than `ParameterValue` and `ColumnValueConverter`
/// because it allows types to customize their behavior based on the database value's data type.
/// A database value's data type is the value returned by the `sqlite3_column_type()` before any
/// type conversions have taken place.
///
/// - note: Columns in SQLite have a type affinity (declared type) while stored values have an
/// individual storage class/data type.  There are rules for conversion which are documented
/// at [Datatypes In SQLite Version 3](https://sqlite.org/datatype3.html).
///
/// For example, `NSNumber` can choose what to store in the database based on the boxed value:
///
/// ```swift
/// extension NSNumber: DatabaseSerializable {
///     public func serialized() -> DatabaseValue {
///         switch CFNumberGetType(self as CFNumber) {
///         case .sInt8Type, .sInt16Type, .sInt32Type, .charType, .shortType, .intType,
///              .sInt64Type, .longType, .longLongType, .cfIndexType, .nsIntegerType:
///             return .integer(int64Value)
///         case .float32Type, .float64Type, .floatType, .doubleType, .cgFloatType:
///             return .real(doubleValue)
///         }
///     }
///
///     public static func deserialize(from value: DatabaseValue) throws -> Self {
///         switch value {
///         case .integer(let i):
///             return self.init(value: i)
///         case .real(let r):
///             return self.init(value: r)
///         default:
///             throw DatabaseError(message: "\(value) is not a number")
///         }
///     }
/// }
/// ```
public protocol DatabaseSerializable {
	/// Returns a serialized value of `self`.
	///
	/// - returns: A serialized value representing `self`
	func serialized() -> DatabaseValue

	/// Deserializes and returns `value` as `Self`.
	///
	/// - parameter value: A serialized value of `Self`
	///
	/// - throws: An error if `value` contains an illegal value for `Self`
	///
	/// - returns: An instance of `Self`
	static func deserialize(from value: DatabaseValue) throws -> Self
}

extension DatabaseSerializable {
	public func bind(toStatement statement: Statement, parameter index: Int) throws {
		try statement.bind(value: serialized(), toParameter: index)
	}
}

extension Row {
	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if the column contains an illegal value.
	///
	/// - returns: The column's value.
	public func value<T: DatabaseSerializable>(forColumn index: Int) throws -> T {
		return try T.deserialize(from: value(ofColumn: index))
	}

	/// Returns the value of the column with name `name`.
	///
	/// - parameter name: The name of the desired column.
	///
	/// - throws: An error if the column wasn't found or contains an illegal value.
	///
	/// - returns: The column's value.
	public func value<T: DatabaseSerializable>(named name: String) throws -> T {
		return try value(forColumn: statement.index(ofColumn: name))
	}
}

extension Statement {
	/// Returns the value of the column at `index` for each row in the result set.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	///
	/// - requires: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds.
	public func values<T: DatabaseSerializable>(forColumn index: Int) throws -> [T] {
		var values = [T]()
		try results { row in
			values.append(try row.value(forColumn: index))
		}
		return values
	}

	/// Returns the value of the column with `name` for each row in the result set.
	///
	/// - parameter name: The name of the desired column.
	///
	/// - throws: An error if the column `name` doesn't exist.
	public func values<T: DatabaseSerializable>(forColumn name: String) throws -> [T] {
		let index = try index(ofColumn: name)
		var values = [T]()
		try results { row in
			values.append(try row.value(forColumn: index))
		}
		return values
	}

	/// Returns the values of the columns at `indexes` for each row in the result set.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	///
	/// - requires: `indexes.min() >= 0`
	/// - requires: `indexes.max() < self.columnCount`
	///
	/// - parameter indexes: The indexes of the desired columns.
	///
	/// - throws: An error if any element of `indexes` is out of bounds.
	public func values<S: Collection, T: DatabaseSerializable>(forColumns indexes: S) throws -> [[T]] where S.Element == Int {
		var values = [[T]](repeating: [], count: indexes.count)
		for (n, x) in indexes.enumerated() {
			values[n] = try self.values(forColumn: x)
		}
		return values
	}

	/// Returns the values of the columns with `names` for each row in the result set.
	///
	/// - parameter names: The names of the desired columns.
	///
	/// - throws: An error if a column in `names` doesn't exist.
	public func values<S: Collection, T: DatabaseSerializable>(forColumns names: S) throws -> [String: [T]] where S.Element == String {
		var values: [String: [T]] = [:]
		for name in names {
			values[name] = try self.values(forColumn: name)
		}
		return values
	}
}

extension NSNumber: DatabaseSerializable {
	public func serialized() -> DatabaseValue {
		switch CFNumberGetType(self as CFNumber) {
		case .sInt8Type, .sInt16Type, .sInt32Type, .charType, .shortType, .intType,
				.sInt64Type, .longType, .longLongType, .cfIndexType, .nsIntegerType:
			return .integer(int64Value)
		case .float32Type, .float64Type, .floatType, .doubleType, .cgFloatType:
			return .real(doubleValue)
		@unknown default:
			fatalError("Unexpected CFNumber type")
		}
	}

	public static func deserialize(from value: DatabaseValue) throws -> Self {
		switch value {
		case .integer(let i):
			return self.init(value: i)
		case .real(let r):
			return self.init(value: r)
		default:
			throw DatabaseError(message: "\(value) is not a number")
		}
	}
}

extension NSNull: DatabaseSerializable {
	public func serialized() -> DatabaseValue {
		return .null
	}

	public static func deserialize(from value: DatabaseValue) throws -> Self {
		switch value {
		case .null:
			return self.init()
		default:
			throw DatabaseError(message: "\(value) is not NULL")
		}
	}
}

extension DatabaseSerializable where Self: NSObject, Self: NSCoding {
	public func serialized() throws -> DatabaseValue {
		return .blob(try NSKeyedArchiver.archivedData(withRootObject: self, requiringSecureCoding: true))
	}

	public static func deserialize(from value: DatabaseValue) throws -> Self {
		switch value {
		case .blob(let b):
			guard let result = try NSKeyedUnarchiver.unarchivedObject(ofClass: Self.self, from: b) else {
				throw DatabaseError(message: "\(b) is not a valid instance of \(Self.self)")
			}
			return result
		default:
			throw DatabaseError(message: "\(value) is not a blob")
		}
	}
}
