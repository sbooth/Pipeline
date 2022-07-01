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
/// 	static var uuid = ColumnValueConverter { row, index in
/// 		let t = try row.text(forColumn: index)
/// 		guard let u = UUID(uuidString: t) else {
/// 			throw DatabaseError(message: "text \"\(t)\" isn't a valid UUID")
/// 		}
/// 		return u
/// 	}
///  ```
struct ColumnValueConverter<T> {
	/// Converts the value at `index` in `row` to `T` and returns the result.
	///
	/// - precondition: `row.type(ofColumn: index) != .null`
	///
	/// - parameter row: A `Row` object containing the desired value.
	/// - parameter index: The index of the desired column.
	/// 
	/// - throws: An error if the type conversion could not be accomplished.
	let convert: (_ row: Row, _ index: Int) throws -> T
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
	func value<T>(forColumn index: Int, as type: T.Type = T.self, _ converter: ColumnValueConverter<T>) throws -> T {
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
	func valueOrNil<T>(forColumn index: Int, as type: T.Type = T.self, _ converter: ColumnValueConverter<T>) throws -> T? {
		if try self.type(ofColumn: index) == .null {
			return nil
		}
		return try converter.convert(self, index)
	}
}

extension ColumnValueConverter where T == String {
	/// Returns the text value of a column.
	static var string = ColumnValueConverter {
		try $0.text(forColumn: $1)
	}
}

extension ColumnValueConverter where T == Data {
	/// Returns the BLOB value of a column.
	static var data = ColumnValueConverter {
		try $0.blob(forColumn: $1)
	}
}

extension ColumnValueConverter where T == Int {
	/// Converts the signed integer value of a column to `Int`.
	static var int = ColumnValueConverter {
		Int(try $0.integer(forColumn: $1))
	}
}

extension ColumnValueConverter where T == UInt {
	/// Converts the signed integer value of a column to `UInt`.
	/// - note: The signed integer value is interpreted as a bit pattern.
	static var uint = ColumnValueConverter {
		UInt(bitPattern: Int(try $0.integer(forColumn: $1)))
	}
}

extension ColumnValueConverter where T == Int8 {
	/// Converts the signed integer value of a column to `Int8`.
	static var int8 = ColumnValueConverter {
		Int8(try $0.integer(forColumn: $1))
	}
}

extension ColumnValueConverter where T == UInt8 {
	/// Converts the signed integer value of a column to `UInt8`.
	static var uint8 = ColumnValueConverter {
		UInt8(try $0.integer(forColumn: $1))
	}
}

extension ColumnValueConverter where T == Int16 {
	/// Converts the signed integer value of a column to `Int16`.
	static var int16 = ColumnValueConverter {
		Int16(try $0.integer(forColumn: $1))
	}
}

extension ColumnValueConverter where T == UInt16 {
	/// Converts the signed integer value of a column to `UInt16`.
	static var uint16 = ColumnValueConverter {
		UInt16(try $0.integer(forColumn: $1))
	}
}

extension ColumnValueConverter where T == Int32 {
	/// Converts the signed integer value of a column to `Int32`.
	static var int32 = ColumnValueConverter {
		Int32(try $0.integer(forColumn: $1))
	}
}

extension ColumnValueConverter where T == UInt32 {
	/// Converts the signed integer value of a column to `UInt32`.
	static var uint32 = ColumnValueConverter {
		UInt32(try $0.integer(forColumn: $1))
	}
}

extension ColumnValueConverter where T == Int64 {
	/// Returns the signed integer value of a column.
	static var int64 = ColumnValueConverter {
		try $0.integer(forColumn: $1)
	}
}

extension ColumnValueConverter where T == UInt64 {
	/// Converts the signed integer value of a column to `UInt64`.
	/// - note: The signed integer value is interpreted as a bit pattern.
	static var uint64 = ColumnValueConverter {
		UInt64(bitPattern: try $0.integer(forColumn: $1))
	}
}

extension ColumnValueConverter where T == Float {
	/// Converts the floating-point value of a column to `Float`.
	static var float = ColumnValueConverter {
		Float(try $0.real(forColumn: $1))
	}
}

extension ColumnValueConverter where T == Double {
	/// Returns the floating-point value of a column.
	static var double = ColumnValueConverter {
		try $0.real(forColumn: $1)
	}
}

extension ColumnValueConverter where T == Bool {
	/// Converts the signed integer value of a column to `Bool`.
	/// - note: Non-zero values are interpreted as true.
	static var bool = ColumnValueConverter {
		try $0.integer(forColumn: $1) != 0
	}
}

extension ColumnValueConverter where T == UUID {
	/// Converts the text value of a column to `UUID`.
	/// - note: The text value is interpreted as a UUID string.
	static var uuid = ColumnValueConverter { row, index in
		let t = try row.text(forColumn: index)
		guard let u = UUID(uuidString: t) else {
			throw DatabaseError(message: "text \"\(t)\" isn't a valid UUID")
		}
		return u
	}

	/// Converts the BLOB value of a column to `UUID`.
	/// - note: The BLOB value is interpreted as a 16-byte `uuid_t`.
	static var uuidWithBytes = ColumnValueConverter { row, index in
		let b = try row.blob(forColumn: index)
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
	static var url = ColumnValueConverter { row, index in
		let t = try row.text(forColumn: index)
		guard let u = URL(string: t) else {
			throw DatabaseError(message: "text \"\(t)\" isn't a valid URL")
		}
		return u
	}
}

extension ColumnValueConverter where T == Date {
	/// Converts the floating-point value of a column to `Date`.
	/// - note: The floating-point value is interpreted as a number of seconds relative to 00:00:00 UTC on 1 January 1970.
	static var dateWithTimeIntervalSince1970 = ColumnValueConverter {
		Date(timeIntervalSince1970: try $0.real(forColumn: $1))
	}

	/// Converts the floating-point value of a column to `Date`.
	/// - note: The floating-point value is interpreted as a number of seconds relative to 00:00:00 UTC on 1 January 2001.
	static var dateWithTimeIntervalSinceReferenceDate = ColumnValueConverter {
		Date(timeIntervalSinceReferenceDate: try $0.real(forColumn: $1))
	}
}

//extension ColumnValueConverter where T: Decodable {
//	static var json = ColumnValueConverter { row, index in
//		let b = try row.blob(forColumn: index)
//		return try JSONDecoder().decode(T.self, from: b)
//	}
//}
