//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

/// A struct responsible for binding a captured value to an SQL parameter.
///
/// The implementation normally uses either `bind(integer:toParameter:)`, `bind(real:toParameter:)`,
/// `bind(text:toParameter:)`, or`bind(blob:toParameter:)` but lower-level SQLite
/// operations are also possible.
///
/// For example, an implementation for binding a `UUID` as text is:
///
/// ```swift
/// extension SQLParameter {
/// 	public static func uuidString(_ value: UUID) -> SQLParameter {
///			SQLParameter { statement, index in
///				try statement.bind(text: value.uuidString.lowercased(), toParameter: index)
/// 		}
/// 	}
///  ```
public struct SQLParameter {
	/// Binds a captured value to the SQL parameter at `index` in `statement`.
	///
	/// - parameter statement: A `Statement` object to receive the desired parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if the value could not be bound.
	public let bind: (_ statement: Statement, _ index: Int) throws -> ()
}

extension Statement {
	/// Binds `value` to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `value` couldn't be bound.
	///
	/// - returns: `self`.
	@discardableResult public func bind(_ value: SQLParameter, toParameter index: Int) throws -> Statement {
		try value.bind(self, index)
		return self
	}

	/// Binds `value` to the SQL parameter `name`.
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter name: The name of the SQL parameter to bind.
	///
	/// - throws: An error if the SQL parameter `name` doesn't exist or `value` couldn't be bound.
	///
	/// - returns: `self`.
	@discardableResult public func bind(_ value: SQLParameter, toParameter name: String) throws -> Statement {
		try value.bind(self, indexOfParameter(name))
		return self
	}
}

extension Statement {
	/// Binds the *n* parameters in `values` to the first *n* SQL parameters of `self`.
	///
	/// - requires: `values.count <= self.parameterCount`.
	///
	/// - parameter values: A collection of values to bind to SQL parameters.
	///
	/// - throws: An error if one of `values` couldn't be bound.
	///
	/// - returns: `self`.
	@discardableResult public func bind<C: Collection>(_ values: C) throws -> Statement where C.Element == SQLParameter {
		var index = 1
		for value in values {
			try value.bind(self, index)
			index += 1
		}
		return self
	}

	/// Binds *value* to SQL parameter *name* for each (*name*, *value*) in `parameters`.
	///
	/// - requires: `parameters.count <= self.parameterCount`.
	///
	/// - parameter parameters: A collection of name and value pairs to bind to SQL parameters.
	///
	/// - throws: An error if the SQL parameter *name* doesn't exist or *value* couldn't be bound.
	///
	/// - returns: `self`.
	@discardableResult public func bind<C: Collection>(_ parameters: C) throws -> Statement where C.Element == (key: String, value: SQLParameter) {
		for (name, value) in parameters {
			try value.bind(self, indexOfParameter(name))
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
	///
	/// - throws: An error if one of `values` couldn't be bound.
	///
	/// - returns: `self`.
	@discardableResult public func bind(_ values: SQLParameter...) throws -> Statement {
		try bind(values)
	}
}

extension Database {
	/// Executes `sql` with the *n* `parameters` bound to the first *n* SQL parameters of `sql` and applies `block` to each result row.
	///
	/// - parameter sql: The SQL statement to execute.
	/// - parameter parameters: A collection of values to bind to SQL parameters.
	/// - parameter block: A closure called for each result row.
	/// - parameter row: A result row of returned data.
	///
	/// - throws: Any error thrown in `block` or an error if `sql` couldn't be compiled, `values` couldn't be bound, or the statement couldn't be executed.
	public func execute<C: Collection>(sql: String, parameters: C, _ block: ((_ row: Row) throws -> ())? = nil) throws where C.Element == SQLParameter {
		let statement = try prepare(sql: sql)
		try statement.bind(parameters)
		if let block = block {
			try statement.results(block)
		} else {
			try statement.execute()
		}
	}

	/// Executes `sql` with *value* bound to SQL parameter *name* for each (*name*, *value*) in `parameters` and applies `block` to each result row.
	///
	/// - parameter sql: The SQL statement to execute.
	/// - parameter parameters: A collection of name and value pairs to bind to SQL parameters.
	/// - parameter block: A closure called for each result row.
	/// - parameter row: A result row of returned data.
	///
	/// - throws: Any error thrown in `block` or an error if `sql` couldn't be compiled, `parameters` couldn't be bound, or the statement couldn't be executed.
	public func execute<C: Collection>(sql: String, parameters: C, _ block: ((_ row: Row) throws -> ())? = nil) throws where C.Element == (key: String, value: SQLParameter) {
		let statement = try prepare(sql: sql)
		try statement.bind(parameters)
		if let block = block {
			try statement.results(block)
		} else {
			try statement.execute()
		}
	}
}

extension Database {
	/// Executes `sql` with the *n* `parameters` bound to the first *n* SQL parameters of `sql` and applies `block` to each result row.
	///
	/// - parameter sql: The SQL statement to execute.
	/// - parameter parameters: A series of values to bind to SQL parameters.
	/// - parameter block: A closure called for each result row.
	/// - parameter row: A result row of returned data.
	///
	/// - throws: Any error thrown in `block` or an error if `sql` couldn't be compiled, `values` couldn't be bound, or the statement couldn't be executed.
	public func execute(sql: String, parameters: SQLParameter..., block: ((_ row: Row) throws -> ())? = nil) throws {
		try execute(sql: sql, parameters: parameters, block)
	}
}

extension SQLParameter {
	/// Binds a `String` object as a text value.
	public static func string(_ value: String) -> SQLParameter {
		SQLParameter { statement, index in
			try statement.bind(text: value, toParameter: index)
		}
	}

	/// Binds an optional `String` object as a text value or SQL NULL if `nil`.
	public static func string(_ value: Optional<String>) -> SQLParameter {
		switch value {
		case .none:
			return .null
		case .some(let obj):
			return .string(obj)
		}
	}
}

extension SQLParameter {
	/// Binds a `Data` object as a BLOB value.
	public static func data(_ value: Data) -> SQLParameter {
		SQLParameter { statement, index in
			try statement.bind(blob: value, toParameter: index)
		}
	}

	/// Binds an optional `Data` object as a BLOB value or SQL NULL if `nil`.
	public static func data(_ value: Optional<Data>) -> SQLParameter {
		switch value {
		case .none:
			return .null
		case .some(let obj):
			return .data(obj)
		}
	}
}

extension SQLParameter {
	/// Binds a SQL NULL value.
	public static let null = SQLParameter { statement, index in
		try statement.bindNull(toParameter: index)
	}
}

extension SQLParameter {
	/// Binds an `Int` object as a signed integer value.
	public static func int(_ value: Int) -> SQLParameter {
		.int64(Int64(value))
	}

	/// Binds a `UInt` object as a signed integer value.
	/// - note: The value is bound as an `Int` bit pattern.
	public static func uint(_ value: UInt) -> SQLParameter {
		.int64(Int64(Int(bitPattern: value)))
	}

	/// Binds an `Int8` object as a signed integer value.
	public static func int8(_ value: Int8) -> SQLParameter {
		.int64(Int64(value))
	}

	/// Binds a `UInt8` object as a signed integer value.
	public static func uint8(_ value: UInt8) -> SQLParameter {
		.int64(Int64(value))
	}

	/// Binds an `Int16` object as a signed integer value.
	public static func int16(_ value: Int16) -> SQLParameter {
		.int64(Int64(value))
	}

	/// Binds a `UInt16` object as a signed integer value.
	public static func uint16(_ value: UInt16) -> SQLParameter {
		.int64(Int64(value))
	}

	/// Binds an `Int32` object as a signed integer value.
	public static func int32(_ value: Int32) -> SQLParameter {
		.int64(Int64(value))
	}

	/// Binds a `UInt32` object as a signed integer value.
	public static func uint32(_ value: UInt32) -> SQLParameter {
		.int64(Int64(value))
	}

	/// Binds an `Int64` object as a signed integer value.
	public static func int64(_ value: Int64) -> SQLParameter {
		SQLParameter { statement, index in
			try statement.bind(integer: value, toParameter: index)
		}
	}

	/// Binds a `UInt64` object as a signed integer value.
	/// - note: The value is bound as an `Int64` bit pattern.
	public static func uint64(_ value: UInt64) -> SQLParameter {
		.int64(Int64(bitPattern: value))
	}

	/// Binds an optional `Int` object as a signed integer value or SQL NULL if `nil`.
	public static func int(_ value: Optional<Int>) -> SQLParameter {
		switch value {
		case .none:
			return .null
		case .some(let obj):
			return .int(obj)
		}
	}

	/// Binds an optional `UInt` object as a signed integer value or SQL NULL if `nil`.
	/// - note: The value is bound as an `Int` bit pattern.
	public static func uint(_ value: Optional<UInt>) -> SQLParameter {
		switch value {
		case .none:
			return .null
		case .some(let obj):
			return .uint(obj)
		}
	}

	/// Binds an optional `Int8` object as a signed integer value or SQL NULL if `nil`.
	public static func int8(_ value: Optional<Int8>) -> SQLParameter {
		switch value {
		case .none:
			return .null
		case .some(let obj):
			return .int8(obj)
		}
	}

	/// Binds an optional `UInt8` object as a signed integer value or SQL NULL if `nil`.
	public static func uint8(_ value: Optional<UInt8>) -> SQLParameter {
		switch value {
		case .none:
			return .null
		case .some(let obj):
			return .uint8(obj)
		}
	}

	/// Binds an optional `Int16` object as a signed integer value or SQL NULL if `nil`.
	public static func int16(_ value: Optional<Int16>) -> SQLParameter {
		switch value {
		case .none:
			return .null
		case .some(let obj):
			return .int16(obj)
		}
	}

	/// Binds an optional `UInt16` object as a signed integer value or SQL NULL if `nil`.
	public static func uint16(_ value: Optional<UInt16>) -> SQLParameter {
		switch value {
		case .none:
			return .null
		case .some(let obj):
			return .uint16(obj)
		}
	}

	/// Binds an optional `Int32` object as a signed integer value or SQL NULL if `nil`.
	public static func int32(_ value: Optional<Int32>) -> SQLParameter {
		switch value {
		case .none:
			return .null
		case .some(let obj):
			return .int32(obj)
		}
	}

	/// Binds an optional `UInt32` object as a signed integer value or SQL NULL if `nil`.
	public static func uint32(_ value: Optional<UInt32>) -> SQLParameter {
		switch value {
		case .none:
			return .null
		case .some(let obj):
			return .uint32(obj)
		}
	}

	/// Binds an optional `Int64` object as a signed integer value or SQL NULL if `nil`.
	public static func int64(_ value: Optional<Int64>) -> SQLParameter {
		switch value {
		case .none:
			return .null
		case .some(let obj):
			return .int64(obj)
		}
	}

	/// Binds an optional `UInt64` object as a signed integer value or SQL NULL if `nil`.
	/// - note: The value is bound as an `Int64` bit pattern.
	public static func uint(_ value: Optional<UInt64>) -> SQLParameter {
		switch value {
		case .none:
			return .null
		case .some(let obj):
			return .uint64(obj)
		}
	}
}

extension SQLParameter {
	/// Binds a `Float` object as a floating-point value.
	public static func float(_ value: Float) -> SQLParameter {
		.double(Double(value))
	}

	/// Binds a `Double` object as a floating-point value.
	public static func double(_ value: Double) -> SQLParameter {
		SQLParameter { statement, index in
			try statement.bind(real: value, toParameter: index)
		}
	}

	/// Binds an optional `Float` object as a floating-point value or SQL NULL if `nil`.
	public static func float(_ value: Optional<Float>) -> SQLParameter {
		switch value {
		case .none:
			return .null
		case .some(let obj):
			return .float(obj)
		}
	}

	/// Binds an optional `Double` object as a floating-point value or SQL NULL if `nil`.
	public static func double(_ value: Optional<Double>) -> SQLParameter {
		switch value {
		case .none:
			return .null
		case .some(let obj):
			return .double(obj)
		}
	}
}

extension SQLParameter {
	/// Binds a `Bool` object as a signed integer value.
	/// - note: True is bound as 1 while false is bound as 0.
	public static func bool(_ value: Bool) -> SQLParameter {
		.int64(value ? 1 : 0)
	}

	/// Binds an optional `Bool` object as a signed integer value or SQL NULL if `nil`.
	/// - note: True is bound as 1 while false is bound as 0.
	public static func bool(_ value: Optional<Bool>) -> SQLParameter {
		switch value {
		case .none:
			return .null
		case .some(let obj):
			return .bool(obj)
		}
	}
}

extension SQLParameter {
	/// Binds a `UUID` object as a text value.
	/// - note: The value is bound as a lower case UUID string.
	public static func uuidString(_ value: UUID) -> SQLParameter {
		.string(value.uuidString.lowercased())
	}

	/// Binds a `UUID` object as a BLOB value.
	/// - note: The value is bound as a 16-byte `uuid_t`.
	public static func uuidBytes(_ value: UUID) -> SQLParameter {
		SQLParameter { statement, index in
			let b = withUnsafeBytes(of: value.uuid) {
				Data($0)
			}
			try statement.bind(blob: b, toParameter: index)
		}
	}

	/// Binds an optional `UUID` object as a text value or SQL NULL if `nil`.
	/// - note: The value is bound as a lower case UUID string.
	public static func uuidString(_ value: Optional<UUID>) -> SQLParameter {
		switch value {
		case .none:
			return .null
		case .some(let obj):
			return .uuidString(obj)
		}
	}

	/// Binds an optional `UUID` object as a BLOB value or SQL NULL if `nil`.
	/// - note: The value is bound as a lower case UUID string.
	public static func uuidBytes(_ value: Optional<UUID>) -> SQLParameter {
		switch value {
		case .none:
			return .null
		case .some(let obj):
			return .uuidBytes(obj)
		}
	}
}

extension SQLParameter {
	/// Binds a `URL` object as a text value.
	public static func urlString(_ value: URL) -> SQLParameter {
		.string(value.absoluteString)
	}

	/// Binds an optional `URL` object as a text value or SQL NULL if `nil`.
	public static func urlString(_ value: Optional<URL>) -> SQLParameter {
		switch value {
		case .none:
			return .null
		case .some(let obj):
			return .urlString(obj)
		}
	}
}

extension SQLParameter {
	/// Binds a `Date` object as a floating-point value.
	/// - note: The value is bound as the number of seconds relative to 00:00:00 UTC on 1 January 1970.
	public static func timeIntervalSince1970(_ value: Date) -> SQLParameter {
		.double(value.timeIntervalSince1970)
	}

	/// Binds a `Date` object as a floating-point value.
	/// - note: The value is bound as the number of seconds relative to 00:00:00 UTC on 1 January 2001.
	public static func timeIntervalSinceReferenceDate(_ value: Date) -> SQLParameter {
		.double(value.timeIntervalSinceReferenceDate)
	}

	/// Binds a `Date` object as a text value.
	/// - parameter formatter: The formatter to use to generate the ISO 8601 date representation.
	public static func iso8601DateString(_ value: Date, formatter: ISO8601DateFormatter = ISO8601DateFormatter()) -> SQLParameter {
		.string(formatter.string(from: value))
	}

	/// Binds an optional `Date` object as a floating-point value or SQL NULL if `nil`.
	/// - note: The value is bound as the number of seconds relative to 00:00:00 UTC on 1 January 1970.
	public static func timeIntervalSince1970(_ value: Optional<Date>) -> SQLParameter {
		switch value {
		case .none:
			return .null
		case .some(let obj):
			return .timeIntervalSince1970(obj)
		}
	}

	/// Binds an optional `Date` object as a floating-point value or SQL NULL if `nil`.
	/// - note: The value is bound as the number of seconds relative to 00:00:00 UTC on 1 January 2001.
	public static func timeIntervalSinceReferenceDate(_ value: Optional<Date>) -> SQLParameter {
		switch value {
		case .none:
			return .null
		case .some(let obj):
			return .timeIntervalSinceReferenceDate(obj)
		}
	}

	/// Binds an optional `Date` object as a text value or SQL NULL if `nil`.
	/// - parameter formatter: The formatter to use to generate the ISO 8601 date representation.
	public static func iso8601DateString(_ value: Optional<Date>, formatter: ISO8601DateFormatter = ISO8601DateFormatter()) -> SQLParameter {
		switch value {
		case .none:
			return .null
		case .some(let obj):
			return .iso8601DateString(obj, formatter: formatter)
		}
	}
}

extension SQLParameter {
	/// Binds an `Encodable` instance as encoded JSON data.
	/// - parameter encoder: The encoder to use to generate the encoded JSON data.
	public static func json<T>(_ value: T, encoder: JSONEncoder = JSONEncoder()) -> SQLParameter where T: Encodable {
		return SQLParameter { statement, index in
			let b = try encoder.encode(value)
			try statement.bind(blob: b, toParameter: index)
		}
	}
}

extension SQLParameter: ExpressibleByNilLiteral {
	/// Binds a SQL NULL value.
	public init(nilLiteral: ()) {
		self = .null
	}
}

extension SQLParameter: ExpressibleByIntegerLiteral {
	/// Binds a signed integer value.
	public init(integerLiteral value: IntegerLiteralType) {
		self = .int64(Int64(value))
	}
}

extension SQLParameter: ExpressibleByFloatLiteral {
	/// Binds a floating-point value.
	public init(floatLiteral value: FloatLiteralType) {
		self = .double(value)
	}
}

extension SQLParameter: ExpressibleByStringLiteral {
	/// Binds a text value.
	public init(stringLiteral value: StringLiteralType) {
		self = .string(value)
	}
}

extension SQLParameter: ExpressibleByBooleanLiteral {
	/// Binds a boolean value as a signed integer value.
	/// - note: True is bound as 1 while false is bound as 0.
	public init(booleanLiteral value: BooleanLiteralType) {
		self = .int64(value ? 1 : 0)
	}
}

extension SQLParameter {
	/// Binds an `NSNumber` object as a signed integer or floating-point value.
	public static func nsNumber(_ value: NSNumber) -> SQLParameter {
		SQLParameter { statement, index in
			switch CFNumberGetType(value as CFNumber) {
			case .sInt8Type, .sInt16Type, .sInt32Type, .charType, .shortType, .intType,
					.sInt64Type, .longType, .longLongType, .cfIndexType, .nsIntegerType:
				try statement.bind(integer: value.int64Value, toParameter: index)
			case .float32Type, .float64Type, .floatType, .doubleType, .cgFloatType:
				try statement.bind(real: value.doubleValue, toParameter: index)
			@unknown default:
				fatalError("Unexpected CFNumber type")
			}
		}
	}
}

extension SQLParameter {
	/// Binds an `NSCoding` instance as keyed archive data using `NSKeyedArchiver`.
	public static func nsKeyedArchive<T>(_ value: T) -> SQLParameter where T: NSObject, T: NSCoding {
		SQLParameter { statement, index in
			let b = try NSKeyedArchiver.archivedData(withRootObject: value, requiringSecureCoding: true)
			try statement.bind(blob: b, toParameter: index)
		}
	}
}
