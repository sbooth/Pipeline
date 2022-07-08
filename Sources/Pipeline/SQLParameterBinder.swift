//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

/// A struct responsible for binding a value to an SQL parameter in a `Statement` object.
///
/// The implementation normally uses either `bind(integer:toParameter:)`, `bind(real:toParameter:)`,
/// `bind(text:toParameter:)`, `bind(blob:toParameter:)`, or `bindNull(toParameter:)` but lower-level SQLite
/// operations are also possible.
///
/// For example, an implementation for binding a `UUID` object as text is:
///
/// ```swift
/// extension SQLParameterBinder where T == UUID {
/// 	public static let uuidString = SQLParameterBinder {
/// 		try $0.bind(text: $1.uuidString.lowercased(), toParameter: $2)
/// 	}
///  ```
public struct SQLParameterBinder<T> {
	/// Binds `value` to the SQL parameter `index` in `statement`.
	///
	/// - parameter statement: A `Statement` object to receive the bound value.
	/// - parameter value: The desired value.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if the value could not be bound.
	public let bind: (_ statement: Statement, _ value: T, _ index: Int) throws -> ()
}

extension Statement {
	/// Binds `value` to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	/// - parameter binder: The `SQLParameterBinder` to use for binding `value`.
	///
	/// - throws: An error if `value` couldn't be bound.
	///
	/// - returns: `self`.
	@discardableResult public func bind<T>(_ value: T, toParameter index: Int, _ binder: SQLParameterBinder<T>) throws -> Statement {
		try binder.bind(self, value, index)
		return self
	}

	/// Binds `value` to the SQL parameter named `name`.
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter name: The name of the SQL parameter to bind.
	/// - parameter binder: The `SQLParameterBinder` to use for binding `value`.
	///
	/// - throws: An error if `value` couldn't be bound.
	///
	/// - returns: `self`.
	@discardableResult public func bind<T>(_ value: T, toParameter name: String, _ binder: SQLParameterBinder<T>) throws -> Statement {
		try bind(value, toParameter: indexOfParameter(name), binder)
	}
}

extension Statement {
	/// Binds `value` or SQL NULL to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	/// - parameter binder: The `SQLParameterBinder` to use for binding `value`.
	///
	/// - throws: An error if `value` couldn't be bound.
	///
	/// - returns: `self`.
	@discardableResult public func bind<T>(_ value: Optional<T>, toParameter index: Int, _ binder: SQLParameterBinder<T>) throws -> Statement {
		switch value {
		case .none:
			try bindNull(toParameter: index)
		case .some(let obj):
			try bind(obj, toParameter: index, binder)
		}
		return self
	}

	/// Binds `value` or SQL NULL to the SQL parameter named `name`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter name: The name of the SQL parameter to bind.
	/// - parameter binder: The `SQLParameterBinder` to use for binding `value`.
	///
	/// - throws: An error if `value` couldn't be bound.
	///
	/// - returns: `self`.
	@discardableResult public func bind<T>(_ value: Optional<T>, toParameter name: String, _ binder: SQLParameterBinder<T>) throws -> Statement {
		try bind(value, toParameter: indexOfParameter(name), binder)
	}
}

extension Statement {
	/// Binds the *n* parameters in `values` to the first *n* SQL parameters of `self`.
	///
	/// - requires: `values.count <= self.parameterCount`.
	///
	/// - parameter values: A collection of values to bind to SQL parameters.
	/// - parameter binder: The `SQLParameterBinder` to use for binding the elements of`values`.
	///
	/// - throws: An error if one of `values` couldn't be bound.
	///
	/// - returns: `self`.
	@discardableResult public func bind<C: Collection>(_ values: C, _ binder: SQLParameterBinder<C.Element>) throws -> Statement {
		var index = 1
		for value in values {
			try bind(value, toParameter: index, binder)
			index += 1
		}
		return self
	}

	/// Binds *value* to SQL parameter *name* for each (*name*, *value*) in `parameters`.
	///
	/// - requires: `parameters.count <= self.parameterCount`.
	///
	/// - parameter parameters: A collection of name and value pairs to bind to SQL parameters.
	/// - parameter binder: The `SQLParameterBinder` to use for binding the elements of`values`.
	///
	/// - throws: An error if the SQL parameter *name* doesn't exist or *value* couldn't be bound.
	///
	/// - returns: `self`.
	@discardableResult public func bind<C: Collection, T>(_ parameters: C, _ binder: SQLParameterBinder<T>) throws -> Statement where C.Element == (key: String, value: T) {
		for (name, value) in parameters {
			try bind(value, toParameter: indexOfParameter(name), binder)
		}
		return self
	}
}

extension Statement {
	/// Binds the *n* parameters in `values` to the first *n* SQL parameters of `self`.
	///
	/// - requires: `values.count <= self.parameterCount`.
	///
	/// - parameter values: A collection of values to bind to SQL parameters.
	/// - parameter binder: The `SQLParameterBinder` to use for binding the elements of`values`.
	///
	/// - throws: An error if one of `values` couldn't be bound.
	///
	/// - returns: `self`.
	@discardableResult public func bind<T>(_ values: T..., binder: SQLParameterBinder<T>) throws -> Statement {
		try bind(values, binder)
	}
}

extension SQLParameterBinder where T == String {
	/// Binds a `String` object to an SQL parameter as a text value.
	public static let string = SQLParameterBinder {
		try $0.bind($1, toParameter: $2)
	}
}

extension SQLParameterBinder where T == Data {
	/// Binds a `Data` object to an SQL parameter as a BLOB value.
	public static let data = SQLParameterBinder {
		try $0.bind($1, toParameter: $2)
	}
}

extension SQLParameterBinder where T == Int {
	/// Binds an `Int` object to an SQL parameter as a signed integer value.
	public static let int = SQLParameterBinder {
		try $0.bind(Int64($1), toParameter: $2)
	}
}

extension SQLParameterBinder where T == UInt {
	/// Binds a `UInt` object to an SQL parameter as a signed integer value.
	/// - note: The unsigned integer value is bound as a bit pattern.
	public static let uint = SQLParameterBinder {
		try $0.bind(Int64(Int(bitPattern: $1)), toParameter: $2)
	}
}

extension SQLParameterBinder where T == Int8 {
	/// Binds an `Int8` object to an SQL parameter as a signed integer value.
	public static let int8 = SQLParameterBinder {
		try $0.bind(Int64($1), toParameter: $2)
	}
}

extension SQLParameterBinder where T == UInt8 {
	/// Binds a `UInt8` object to an SQL parameter as a signed integer value.
	public static let uint8 = SQLParameterBinder {
		try $0.bind(Int64($1), toParameter: $2)
	}
}

extension SQLParameterBinder where T == Int16 {
	/// Binds an `Int16` object to an SQL parameter as a signed integer value.
	public static let int16 = SQLParameterBinder {
		try $0.bind(Int64($1), toParameter: $2)
	}
}

extension SQLParameterBinder where T == UInt16 {
	/// Binds a `UInt16` object to an SQL parameter as a signed integer value.
	public static let uint16 = SQLParameterBinder {
		try $0.bind(Int64($1), toParameter: $2)
	}
}

extension SQLParameterBinder where T == Int32 {
	/// Binds an `Int32` object to an SQL parameter as a signed integer value.
	public static let int32 = SQLParameterBinder {
		try $0.bind(Int64($1), toParameter: $2)
	}
}

extension SQLParameterBinder where T == UInt32 {
	/// Binds a `UInt32` object to an SQL parameter as a signed integer value.
	public static let uint32 = SQLParameterBinder {
		try $0.bind(Int64($1), toParameter: $2)
	}
}

extension SQLParameterBinder where T == Int64 {
	/// Binds an `Int64` object to an SQL parameter as a signed integer value.
	public static let int64 = SQLParameterBinder {
		try $0.bind($1, toParameter: $2)
	}
}

extension SQLParameterBinder where T == UInt64 {
	/// Binds a `UInt64` object to an SQL parameter as a signed integer value.
	/// - note: The unsigned integer value is interpreted as a bit pattern.
	public static let uint64 = SQLParameterBinder {
		try $0.bind(Int64(bitPattern: $1), toParameter: $2)
	}
}

extension SQLParameterBinder where T == Float {
	/// Binds a `Float` object to an SQL parameter as a floating-point value.
	public static let float = SQLParameterBinder {
		try $0.bind(Double($1), toParameter: $2)
	}
}

extension SQLParameterBinder where T == Double {
	/// Binds a `Double` object to an SQL parameter as a floating-point value.
	public static let double = SQLParameterBinder {
		try $0.bind($1, toParameter: $2)
	}
}

extension SQLParameterBinder where T == Bool {
	/// Binds a `Bool` object to an SQL parameter as a signed integer value.
	/// - note: True is bound as 1 while false is bound as 0.
	public static let bool = SQLParameterBinder {
		try $0.bind($1 ? Int64(1) : Int64(0), toParameter: $2)
	}
}

extension SQLParameterBinder where T == UUID {
	/// Binds a `UUID` object as a text value.
	/// - note: The value is bound as a lower case UUID string.
	public static let uuidString = SQLParameterBinder {
		try $0.bind($1.uuidString.lowercased(), toParameter: $2)
	}

	/// Binds a `UUID` object as a text value.
	/// - note: The value is bound as a 16-byte `uuid_t`.
	public static let uuidBytes = SQLParameterBinder {
		let b = withUnsafeBytes(of: $1.uuid) {
			Data($0)
		}
		try $0.bind(b, toParameter: $2)
	}
}

extension SQLParameterBinder where T == URL {
	/// Binds a `URL` object as a text value.
	public static let urlString = SQLParameterBinder {
		try $0.bind($1.absoluteString, toParameter: $2)
	}
}

extension SQLParameterBinder where T == Date {
	/// Binds a `Date` object as a floating-point value.
	/// - note: The value is bound as the number of seconds relative to 00:00:00 UTC on 1 January 1970.
	public static let timeIntervalSince1970 = SQLParameterBinder {
		try $0.bind($1.timeIntervalSince1970, toParameter: $2)
	}

	/// Binds a `Date` object as a floating-point value.
	/// - note: The value is bound as the number of seconds relative to 00:00:00 UTC on 1 January 2001.
	public static let timeIntervalSinceReferenceDate = SQLParameterBinder {
		try $0.bind($1.timeIntervalSinceReferenceDate, toParameter: $2)
	}

	/// Binds a `Date` object as a text value.
	/// - parameter formatter: The formatter to use to generate the ISO 8601 date representation.
	public static func iso8601DateString(_ formatter: ISO8601DateFormatter = ISO8601DateFormatter()) -> SQLParameterBinder {
		SQLParameterBinder {
			try $0.bind(formatter.string(from: $1), toParameter: $2)
		}
	}
}

extension SQLParameterBinder where T: Encodable {
	/// Binds an `Encodable` instance as encoded JSON data.
	/// - parameter encoder: The encoder to use to generate the encoded JSON data.
	public static func json(_ encoder: JSONEncoder = JSONEncoder()) -> SQLParameterBinder {
		SQLParameterBinder {
			try $0.bind(encoder.encode($1), toParameter: $2)
		}
	}
}

extension SQLParameterBinder where T == NSNumber {
	/// Binds an `NSNumber` object as a signed integer or floating-point value.
	public static let nsNumber = SQLParameterBinder { statement, value, index in
		switch CFNumberGetType(value as CFNumber) {
		case .sInt8Type, .sInt16Type, .sInt32Type, .charType, .shortType, .intType,
				.sInt64Type, .longType, .longLongType, .cfIndexType, .nsIntegerType:
			try statement.bind(value.int64Value, toParameter: index)
		case .float32Type, .float64Type, .floatType, .doubleType, .cgFloatType:
			try statement.bind(value.doubleValue, toParameter: index)
		@unknown default:
			fatalError("Unexpected CFNumber type")
		}
	}
}

extension SQLParameterBinder where T: NSObject, T: NSCoding {
	/// Binds an `NSCoding` instance as keyed archive data using `NSKeyedArchiver`.
	public static func nsKeyedArchive() -> SQLParameterBinder {
		SQLParameterBinder { statement, value, index in
			let b = try NSKeyedArchiver.archivedData(withRootObject: value, requiringSecureCoding: true)
			try statement.bind(b, toParameter: index)
		}
	}
}
