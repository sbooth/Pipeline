//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

/// A struct responsible for binding a value to an SQL parameter.
///
/// The implementation normally uses either `bind(integer:toParameter:)`, `bind(real:toParameter:)`,
/// `bind(text:toParameter:)`, or`bind(blob:toParameter:)` but lower-level SQLite
/// operations are also possible.
///
/// For example, an implementation for binding a`UUID` as text is:
///
/// ```swift
/// extension ParameterValueBinder where T == UUID {
/// 	public static var uuid = ParameterValueBinder { statement, value, index in
/// 		try statement.bind(text: value.uuidString.lowercased(), toParameter: index)
/// 	}
///  ```
public struct ParameterValueBinder<T> {
	/// Binds `value` to the SQL parameter at `index` in `statement`.
	///
	/// - parameter statement: A `Statement` object to receive the desired parameter.
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if the type conversion could not be accomplished.
	public let bind: (_ statement: Statement, _ value: T, _ index: Int) throws -> ()
}

extension Statement {
	/// Binds `value` to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	/// - parameter binder: The `ParameterValueBinder` to use for binding `value` to an SQLite fundamental type.
	///
	/// - throws: An error if `index` is out of bounds or type conversion could not be accomplished.
	public func bind<T>(value: T, toParameter index: Int, _ binder: ParameterValueBinder<T>) throws {
		try binder.bind(self, value, index)
	}
}

extension Statement {
	/// Returns the value of the column with name `name` converted to `type`.
	///
	/// - parameter name: The name of the desired column.
	/// - parameter type: The desired value type.
	/// - parameter binder: The `ParameterValueBinder` to use for converting the SQLite fundamental type to `type`.
	///
	/// - throws: An error if the parameter doesn't exist or type conversion could not be accomplished.
	public func bind<T>(value: T, toParameter name: String, _ binder: ParameterValueBinder<T>) throws {
		try bind(value: value, toParameter: indexOfParameter(named: name), binder)
	}
}

extension ParameterValueBinder where T == String {
	/// Binds a text value to an SQL parameter.
	public static var string = ParameterValueBinder { statement, value, index in
		try statement.bind(text: value, toParameter: index)
	}
}

extension ParameterValueBinder where T == Data {
	/// Binds a BLOB to an SQL parameter.
	public static var data = ParameterValueBinder { statement, value, index in
		try statement.bind(blob: value, toParameter: index)
	}
}

extension ParameterValueBinder where T == Int {
	/// Binds an `Int` to an SQL parameter as a signed integer.
	public static var int = ParameterValueBinder { statement, value, index in
		try statement.bind(integer: Int64(value), toParameter: index)
	}
}

extension ParameterValueBinder where T == UInt {
	/// Binds an `UInt` to an SQL parameter as a signed integer.
	/// - note: The value is bound as an `Int` bit pattern.
	public static var uint = ParameterValueBinder { statement, value, index in
		try statement.bind(integer: Int64(Int(bitPattern: value)), toParameter: index)
	}
}

extension ParameterValueBinder where T == Int8 {
	/// Binds an `Int8` to an SQL parameter as a signed integer.
	public static var int8 = ParameterValueBinder { statement, value, index in
		try statement.bind(integer: Int64(value), toParameter: index)
	}
}

extension ParameterValueBinder where T == UInt8 {
	/// Binds an `UInt8` to an SQL parameter as a signed integer.
	public static var uint8 = ParameterValueBinder { statement, value, index in
		try statement.bind(integer: Int64(value), toParameter: index)
	}
}

extension ParameterValueBinder where T == Int16 {
	/// Binds an `Int16` to an SQL parameter as a signed integer.
	public static var int16 = ParameterValueBinder { statement, value, index in
		try statement.bind(integer: Int64(value), toParameter: index)
	}
}

extension ParameterValueBinder where T == UInt16 {
	/// Binds an `UInt16` to an SQL parameter as a signed integer.
	public static var uint16 = ParameterValueBinder { statement, value, index in
		try statement.bind(integer: Int64(value), toParameter: index)
	}
}

extension ParameterValueBinder where T == Int32 {
	/// Binds an `Int32` to an SQL parameter as a signed integer.
	public static var int32 = ParameterValueBinder { statement, value, index in
		try statement.bind(integer: Int64(value), toParameter: index)
	}
}

extension ParameterValueBinder where T == UInt32 {
	/// Binds an `UInt32` as a signed integer value.
	public static var uint32 = ParameterValueBinder { statement, value, index in
		try statement.bind(integer: Int64(value), toParameter: index)
	}
}

extension ParameterValueBinder where T == Int64 {
	/// Binds a signed integer value to an SQL parameter.
	public static var int64 = ParameterValueBinder { statement, value, index in
		try statement.bind(integer: value, toParameter: index)
	}
}

extension ParameterValueBinder where T == UInt64 {
	/// Binds an `UInt64` to an SQL parameter as a signed integer.
	/// - note: The value is bound as an `Int64` bit pattern.
	public static var uint64 = ParameterValueBinder { statement, value, index in
		try statement.bind(integer: Int64(bitPattern: value), toParameter: index)
	}
}

extension ParameterValueBinder where T == Float {
	/// Binds a `Float` to an SQL parameter as a floating-point value.
	public static var float = ParameterValueBinder { statement, value, index in
		try statement.bind(real: Double(value), toParameter: index)
	}
}

extension ParameterValueBinder where T == Double {
	/// Binds a floating-point value to an SQL parameter.
	public static var double = ParameterValueBinder { statement, value, index in
		try statement.bind(real: value, toParameter: index)
	}
}

extension ParameterValueBinder where T == Bool {
	/// Binds a `Bool` to an SQL parameter as a signed integer.
	/// - note: True is bound as 1 while false is bound as 0.
	public static var bool = ParameterValueBinder { statement, value, index in
		try statement.bind(integer: value ? 1 : 0, toParameter: index)
	}
}

extension ParameterValueBinder where T == UUID {
	/// Binds a `UUID` to an SQL parameter as text.
	/// - note: The text value is bound as a lower case UUID string.
	public static var uuid = ParameterValueBinder { statement, value, index in
		try statement.bind(text: value.uuidString.lowercased(), toParameter: index)
	}

	/// Binds a `UUID` to an SQL parameter as a BLOB.
	/// - note: The value is bound as a 16-byte `uuid_t`.
	public static var uuidBytes = ParameterValueBinder { statement, value, index in
		let b = withUnsafeBytes(of: value.uuid) {
			Data($0)
		}
		try statement.bind(blob: b, toParameter: index)
	}
}

extension ParameterValueBinder where T == URL {
	/// Binds a `URL` to an SQL parameter as text.
	/// - note: The text value is interpreted as a URL string.
	public static var url = ParameterValueBinder { statement, value, index in
		try statement.bind(text: value.absoluteString, toParameter: index)
	}
}

extension ParameterValueBinder where T == Date {
	/// Binds a `Date` to an SQL parameter as a floating-point value.
	/// - note: The value is bound as the number of seconds relative to 00:00:00 UTC on 1 January 1970.
	public static var timeIntervalSince1970 = ParameterValueBinder { statement, value, index in
		try statement.bind(real: value.timeIntervalSince1970, toParameter: index)
	}

	/// Binds a `Date` to an SQL parameter as a floating-point value.
	/// - note: The value is bound as the number of seconds relative to 00:00:00 UTC on 1 January 2001.
	public static var timeIntervalSinceReferenceDate = ParameterValueBinder { statement, value, index in
		try statement.bind(real: value.timeIntervalSinceReferenceDate, toParameter: index)
	}
}
