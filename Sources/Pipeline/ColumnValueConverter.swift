//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

/// A struct responsible for converting the value of a column in a result row to `T`.
///
/// The implementation normally uses either `integer(forColumn:)`, `real(forColumn:)`,
/// `text(forColumn:)`, or`blob(forColumn:)` but lower-level SQLite
/// operations are also possible.
///
/// For example, an implementation for `UUID` conversion from text is:
///
/// ```swift
/// extension ColumnValueConverter where T == UUID {
/// 	public static var uuid = ColumnValueConverter { row, index in
/// 		let t = try row.text(forColumn: index)
/// 		guard let u = UUID(uuidString: t) else {
/// 			throw DatabaseError(message: "text \"\(t)\" isn't a valid UUID")
/// 		}
/// 		return u
/// 	}
///  ```
public struct ColumnValueConverter<T> {
	/// Converts the value at `index` in `row` to `T` and returns the result.
	///
	/// - precondition: `row.type(ofColumn: index) != .null`
	///
	/// - parameter row: A `Row` object containing the desired value.
	/// - parameter index: The index of the desired column.
	/// 
	/// - throws: An error if the type conversion could not be accomplished.
	public let convert: (_ row: Row, _ index: Int) throws -> T
}

extension Row {
	/// Returns the value of the column at `index` converted to `type`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	/// - note: Automatic type conversion may be performed by SQLite depending on the column's initial data type.
	///
	/// - parameter index: The index of the desired column.
	/// - parameter type: The desired value type.
	/// - parameter converter: The `ColumnValueConverter` to use for converting the SQLite fundamental type to `type`.
	///
	/// - throws: An error if `index` is out of bounds, the column contains a null value, or type conversion could not be accomplished.
	///
	/// - returns: The column's value as `type`.
	public func value<T>(forColumn index: Int, as type: T.Type = T.self, _ converter: ColumnValueConverter<T>) throws -> T {
		guard try self.type(ofColumn: index) != .null else {
			throw DatabaseError(message: "SQL NULL encountered for column \(index)")
		}
		return try converter.convert(self, index)
	}

	/// Returns the value of the column at `index` converted to `type`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	/// - note: Automatic type conversion may be performed by SQLite depending on the column's initial data type.
	///
	/// - parameter index: The index of the desired column.
	/// - parameter type: The desired value type.
	/// - parameter converter: The `ColumnValueConverter` to use for converting the SQLite fundamental type to `type`.
	///
	/// - throws: An error if `index` is out of bounds or type conversion could not be accomplished.
	///
	/// - returns: The column's value as `type` or `nil` if null.
	public func valueOrNil<T>(forColumn index: Int, as type: T.Type = T.self, _ converter: ColumnValueConverter<T>) throws -> T? {
		if try self.type(ofColumn: index) == .null {
			return nil
		}
		return try converter.convert(self, index)
	}
}

extension Row {
	/// Returns the value of the column with name `name` converted to `type`.
	///
	/// - note: Automatic type conversion may be performed by SQLite depending on the column's initial data type.
	///
	/// - parameter name: The name of the desired column.
	/// - parameter type: The desired value type.
	/// - parameter converter: The `ColumnValueConverter` to use for converting the SQLite fundamental type to `type`.
	///
	/// - throws: An error if the column doesn't exist, the column contains a null value, or type conversion could not be accomplished.
	///
	/// - returns: The column's value as `type`.
	public func value<T>(forColumn name: String, as type: T.Type = T.self, _ converter: ColumnValueConverter<T>) throws -> T {
		try value(forColumn: statement.index(ofColumn: name), as: type, converter)
	}

	/// Returns the value of the column with name `name` converted to `type`.
	///
	/// - note: Automatic type conversion may be performed by SQLite depending on the column's initial data type.
	///
	/// - parameter name: The name of the desired column.
	/// - parameter type: The desired value type.
	/// - parameter converter: The `ColumnValueConverter` to use for converting the SQLite fundamental type to `type`.
	///
	/// - throws: An error if the column doesn't exist or type conversion could not be accomplished.
	///
	/// - returns: The column's value as `type` or `nil` if null.
	public func valueOrNil<T>(forColumn name: String, as type: T.Type = T.self, _ converter: ColumnValueConverter<T>) throws -> T? {
		try valueOrNil(forColumn: statement.index(ofColumn: name), as: type, converter)
	}
}

extension Statement {
	/// Returns the value of the column at `index` converted to `type` for each row in the result set.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	///
	/// - parameter index: The index of the desired column.
	/// - parameter type: The desired value type.
	/// - parameter converter: The `ColumnValueConverter` to use for converting the SQLite fundamental type to `type`.
	///
	/// - throws: An error if `index` is out of bounds, the column contains a null value, or type conversion could not be accomplished.
	///
	/// - returns: The column's values as an array of `type`.
	public func values<T>(forColumn index: Int, as type: T.Type = T.self, _ converter: ColumnValueConverter<T>) throws -> [T] {
		var values = [T]()
		try results { row in
			values.append(try row.value(forColumn: index, as: type, converter))
		}
		return values
	}

	/// Returns the value of the column with `name` converted to `type` for each row in the result set.
	///
	/// - parameter name: The name of the desired column.
	/// - parameter type: The desired value type.
	/// - parameter converter: The `ColumnValueConverter` to use for converting the SQLite fundamental type to `type`.
	///
	/// - throws: An error if the column doesn't exist, the column contains a null value, or type conversion could not be accomplished.
	///
	/// - returns: The column's values as an array of `type`.
	public func values<T>(forColumn name: String, as type: T.Type = T.self, _ converter: ColumnValueConverter<T>) throws -> [T] {
		try values(forColumn: try index(ofColumn: name), as: type, converter)
	}
}

extension Statement {
	/// Returns the values of the column at `indexes` converted to `type` for each row in the result set.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	///
	/// - requires: `indexes.min() >= 0`
	/// - requires: `indexes.max() < self.columnCount`
	///
	/// - parameter indexes: The indexes of the desired column.
	/// - parameter type: The desired value type.
	/// - parameter converter: The `ColumnValueConverter` to use for converting the SQLite fundamental type to `type`.
	///
	/// - throws: An error if any element of `indexes` is out of bounds, the column contains a null value, or type conversion could not be accomplished.
	///
	/// - returns: The column's values as an array of arrays of `type`.
	public func values<S: Collection, T>(forColumns indexes: S, as type: T.Type = T.self, _ converter: ColumnValueConverter<T>) throws -> [[T]] where S.Element == Int {
		var values = [[T]](repeating: [], count: indexes.count)
		for (n, x) in indexes.enumerated() {
			values[n] = try self.values(forColumn: x, as: type, converter)
		}
		return values
	}

	/// Returns the values of the column with `names` converted to `type` for each row in the result set.
	///
	/// - parameter names: The names of the desired columns.
	/// - parameter type: The desired value type.
	/// - parameter converter: The `ColumnValueConverter` to use for converting the SQLite fundamental type to `type`.
	///
	/// - throws: An error if any element of `names` doesn't exist, the column contains a null value, or type conversion could not be accomplished.
	///
	/// - returns: The column's values as a dictionary of arrays of `type` keyed by column name.
	public func values<S: Collection, T>(forColumns names: S, as type: T.Type = T.self, _ converter: ColumnValueConverter<T>) throws -> [String: [T]] where S.Element == String {
		var values: [String: [T]] = [:]
		for name in names {
			values[name] = try self.values(forColumn: name, as: type, converter)
		}
		return values
	}
}

extension ColumnValueConverter where T == String {
	/// Returns the text value of a column.
	public static var string = ColumnValueConverter {
		try $0.text(at: $1)
	}

	/// Returns the text value of a column.
	public static let text = Self.string
}

extension ColumnValueConverter where T == Data {
	/// Returns the BLOB value of a column.
	public static var data = ColumnValueConverter {
		try $0.blob(at: $1)
	}

	/// Returns the BLOB value of a column.
	public static let blob = Self.data
}

extension ColumnValueConverter where T == Int {
	/// Converts the signed integer value of a column to `Int`.
	public static var int = ColumnValueConverter {
		Int(try $0.integer(at: $1))
	}
}

extension ColumnValueConverter where T == UInt {
	/// Converts the signed integer value of a column to `UInt`.
	/// - note: The signed integer value is interpreted as a bit pattern.
	public static var uint = ColumnValueConverter {
		UInt(bitPattern: Int(try $0.integer(at: $1)))
	}
}

extension ColumnValueConverter where T == Int8 {
	/// Converts the signed integer value of a column to `Int8`.
	public static var int8 = ColumnValueConverter {
		Int8(try $0.integer(at: $1))
	}
}

extension ColumnValueConverter where T == UInt8 {
	/// Converts the signed integer value of a column to `UInt8`.
	public static var uint8 = ColumnValueConverter {
		UInt8(try $0.integer(at: $1))
	}
}

extension ColumnValueConverter where T == Int16 {
	/// Converts the signed integer value of a column to `Int16`.
	public static var int16 = ColumnValueConverter {
		Int16(try $0.integer(at: $1))
	}
}

extension ColumnValueConverter where T == UInt16 {
	/// Converts the signed integer value of a column to `UInt16`.
	public static var uint16 = ColumnValueConverter {
		UInt16(try $0.integer(at: $1))
	}
}

extension ColumnValueConverter where T == Int32 {
	/// Converts the signed integer value of a column to `Int32`.
	public static var int32 = ColumnValueConverter {
		Int32(try $0.integer(at: $1))
	}
}

extension ColumnValueConverter where T == UInt32 {
	/// Converts the signed integer value of a column to `UInt32`.
	public static var uint32 = ColumnValueConverter {
		UInt32(try $0.integer(at: $1))
	}
}

extension ColumnValueConverter where T == Int64 {
	/// Returns the signed integer value of a column.
	public static var int64 = ColumnValueConverter {
		try $0.integer(at: $1)
	}

	/// Returns the signed integer value of a column.
	public static let integer = Self.int64
}

extension ColumnValueConverter where T == UInt64 {
	/// Converts the signed integer value of a column to `UInt64`.
	/// - note: The signed integer value is interpreted as a bit pattern.
	public static var uint64 = ColumnValueConverter {
		UInt64(bitPattern: try $0.integer(at: $1))
	}
}

extension ColumnValueConverter where T == Float {
	/// Converts the floating-point value of a column to `Float`.
	public static var float = ColumnValueConverter {
		Float(try $0.real(at: $1))
	}
}

extension ColumnValueConverter where T == Double {
	/// Returns the floating-point value of a column.
	public static var double = ColumnValueConverter {
		try $0.real(at: $1)
	}

	/// Returns the floating-point value of a column.
	public static let real = Self.double
}

extension ColumnValueConverter where T == Bool {
	/// Converts the signed integer value of a column to `Bool`.
	/// - note: Non-zero values are interpreted as true.
	public static var bool = ColumnValueConverter {
		try $0.integer(at: $1) != 0
	}
}

extension ColumnValueConverter where T == UUID {
	/// Converts the text value of a column to `UUID`.
	/// - note: The text value is interpreted as a UUID string.
	public static var uuidWithString = ColumnValueConverter { row, index in
		let t = try row.text(at: index)
		guard let u = UUID(uuidString: t) else {
			throw DatabaseError(message: "text \"\(t)\" isn't a valid UUID")
		}
		return u
	}

	/// Converts the BLOB value of a column to `UUID`.
	/// - note: The BLOB value is interpreted as a 16-byte `uuid_t`.
	public static var uuidWithBytes = ColumnValueConverter { row, index in
		let b = try row.blob(at: index)
		guard b.count == 16 else {
			throw DatabaseError(message: "BLOB '\(b)' isn't a valid UUID")
		}
		let bytes = b.withUnsafeBytes {
			$0.load(as: uuid_t.self)
		}
		return UUID(uuid: bytes)
	}
}

extension ColumnValueConverter where T == URL {
	/// Converts the text value of a column to `URL`.
	/// - note: The text value is interpreted as a URL string.
	public static var urlWithString = ColumnValueConverter { row, index in
		let t = try row.text(at: index)
		guard let u = URL(string: t) else {
			throw DatabaseError(message: "text \"\(t)\" isn't a valid URL")
		}
		return u
	}
}

extension ColumnValueConverter where T == Date {
	/// Converts the floating-point value of a column to `Date`.
	/// - note: The floating-point value is interpreted as a number of seconds relative to 00:00:00 UTC on 1 January 1970.
	public static var dateWithTimeIntervalSince1970 = ColumnValueConverter {
		Date(timeIntervalSince1970: try $0.real(at: $1))
	}

	/// Converts the floating-point value of a column to `Date`.
	/// - note: The floating-point value is interpreted as a number of seconds relative to 00:00:00 UTC on 1 January 2001.
	public static var dateWithTimeIntervalSinceReferenceDate = ColumnValueConverter {
		Date(timeIntervalSinceReferenceDate: try $0.real(at: $1))
	}

	/// Converts the text value of a column to `Date`.
	/// - note: The text value is interpreted as an ISO 8601 date representation.
	public static func dateWithISO8601DateString(formatter: ISO8601DateFormatter = ISO8601DateFormatter()) -> ColumnValueConverter {
		ColumnValueConverter { row, index in
			let t = try row.text(at: index)
			guard let date = formatter.date(from: t) else {
				throw DatabaseError(message: "text \"\(t)\" isn't a valid ISO 8601 date representation")
			}
			return date
		}
	}
}

extension ColumnValueConverter where T: Decodable {
	/// Converts the BLOB value of a column to a `Decodable` instance.
	/// - note: The BLOB value is interpreted  as encoded JSON data of `type`.
	public static func json(_ type: T.Type = T.self, decoder: JSONDecoder = JSONDecoder()) -> ColumnValueConverter {
		ColumnValueConverter { row, index in
			let b = try row.blob(at: index)
			return try decoder.decode(type, from: b)
		}
	}
}

extension ColumnValueConverter where T == NSNumber {
	/// Converts the signed integer or floating-point value of a column to `NSNumber`.
	public static var nsNumber = ColumnValueConverter {
		let type = try $0.type(ofColumn: $1)
		switch type {
		case .integer:
			return NSNumber(value: try $0.integer(at: $1))
		case .real:
			return NSNumber(value: try $0.real(at: $1))
		default:
			throw DatabaseError(message: "\(type) is not a number")
		}
	}
}

extension ColumnValueConverter where T: NSObject, T: NSCoding {
	/// Converts the BLOB value of a column to an `NSCoding` instance using `NSKeyedUnarchiver`.
	public static func nsKeyedArchive(_ type: T.Type = T.self) -> ColumnValueConverter {
		ColumnValueConverter { row, index in
			let b = try row.blob(at: index)
			guard let result = try NSKeyedUnarchiver.unarchivedObject(ofClass: type, from: b) else {
				throw DatabaseError(message: "\(b) is not a valid instance of \(type)")
			}
			return result
		}
	}
}
