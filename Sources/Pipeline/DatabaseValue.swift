//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

/// A fundamental data type value in an SQLite database.
///
/// - seealso: [Datatypes In SQLite](https://sqlite.org/datatype3.html)
public enum DatabaseValue {
	/// A signed integer value.
	case integer(Int64)
	/// A floating-point value.
	case real(Double)
	/// A text value.
	case text(String)
	/// A BLOB (untyped bytes) value.
	case blob(Data)
	/// A null value.
	case null
}

extension DatabaseValue: Equatable {
	public static func == (lhs: DatabaseValue, rhs: DatabaseValue) -> Bool {
		switch (lhs, rhs) {
		case (.integer(let i1), .integer(let i2)):
			return i1 == i2
		case (.real(let r1), .real(let r2)):
			return r1 == r2
		case (.text(let t1), .text(let t2)):
			return t1 == t2
		case (.blob(let b1), .blob(let b2)):
			return b1 == b2
		case (.null, .null):
			// SQL NULL compares unequal to everything, including other NULL values.
			// Is that really the desired behavior here?
			return false
		default:
			return false
		}
	}
}

extension DatabaseValue {
	/// Returns `true` if self is `.null`.
	public var isNull: Bool {
		if case .null = self {
			return true
		}
		return false
	}
}

extension DatabaseValue: ExpressibleByNilLiteral {
	public init(nilLiteral: ()) {
		self = .null
	}
}

extension DatabaseValue: ExpressibleByIntegerLiteral {
	public init(integerLiteral value: IntegerLiteralType) {
		self = .integer(Int64(value))
	}
}

extension DatabaseValue: ExpressibleByFloatLiteral {
	public init(floatLiteral value: FloatLiteralType) {
		self = .real(value)
	}
}

extension DatabaseValue: ExpressibleByStringLiteral {
	public init(stringLiteral value: StringLiteralType) {
		self = .text(value)
	}
}

extension DatabaseValue: ExpressibleByBooleanLiteral {
	public init(booleanLiteral value: BooleanLiteralType) {
		self = .integer(value ? 1 : 0)
	}
}

extension DatabaseValue: CustomStringConvertible {
	/// A description of the type and value of `self`.
	public var description: String {
		switch self {
		case .integer(let i):
			return ".integer(\(i))"
		case .real(let r):
			return ".real(\(r))"
		case .text(let t):
			return ".text(\"\(t)\")"
		case .blob(let b):
			return ".blob(\(b))"
		case .null:
			return ".null"
		}
	}
}

extension DatabaseValue {
	/// Creates a database value initialized to a signed integer value or null.
	public init(_ value: Int64?) {
		switch value {
		case .none:
			self = .null
		case .some(let int64):
			self = .integer(int64)
		}
	}

	/// Creates a database value initialized to a floating-point value or null.
	public init(_ value: Double?) {
		switch value {
		case .none:
			self = .null
		case .some(let double):
			self = .real(double)
		}
	}

	/// Creates a database value initialized to a text value or null.
	public init(_ value: String?) {
		switch value {
		case .none:
			self = .null
		case .some(let string):
			self = .text(string)
		}
	}

	/// Creates a database value initialized to a BLOB value or null.
	public init(_ value: Data?) {
		switch value {
		case .none:
			self = .null
		case .some(let data):
			self = .blob(data)
		}
	}
}

extension DatabaseValue {
	/// Creates and returns  a database value containing a signed integer value.
	public static func int(_ value: Int) -> DatabaseValue {
		.integer(Int64(value))
	}

	/// Creates and returns  a database value containing the signed integer value of an unsigned integer.
	/// - note: The database value contains a bit pattern.
	public static func uint(_ value: UInt) -> DatabaseValue {
		.integer(Int64(Int(bitPattern: value)))
	}


	/// Creates and returns  a database value containing a signed integer value.
	public static func int8(_ value: Int8) -> DatabaseValue {
		.integer(Int64(value))
	}

	/// Creates and returns  a database value containing the signed integer value of an unsigned 8-bit integer.
	public static func uint8(_ value: UInt8) -> DatabaseValue {
		.integer(Int64(value))
	}


	/// Creates and returns  a database value containing a signed integer value.
	public static func int16(_ value: Int16) -> DatabaseValue {
		.integer(Int64(value))
	}

	/// Creates and returns  a database value containing the signed integer value of a 16-bit unsigned integer.
	public static func uint16(_ value: UInt16) -> DatabaseValue {
		.integer(Int64(value))
	}


	/// Creates and returns  a database value containing a signed integer value.
	public static func int32(_ value: Int32) -> DatabaseValue {
		.integer(Int64(value))
	}

	/// Creates and returns  a database value containing the signed integer value of a 32-bit unsigned integer.
	public static func uint32(_ value: UInt32) -> DatabaseValue {
		.integer(Int64(value))
	}


	/// Creates and returns  a database value containing a signed integer value.
	public static func int64(_ value: Int64) -> DatabaseValue {
		.integer(value)
	}

	/// Creates and returns  a database value containing a signed integer representation of a 64-bit unsigned integer.
	/// - note: The database value contains a bit pattern.
	public static func uint64(_ value: UInt64) -> DatabaseValue {
		.integer(Int64(bitPattern: value))
	}
}

extension DatabaseValue {
	/// Creates a database value initialized to a signed integer value or null.
	public static func int(_ value: Int?) -> DatabaseValue {
		switch value {
		case .none:
			return .null
		case .some(let int):
			return .integer(Int64(int))
		}
	}

	/// Creates a database value initialized to a signed integer value or null.
	public static func uint(_ value: UInt?) -> DatabaseValue {
		switch value {
		case .none:
			return .null
		case .some(let uint):
			return .integer(Int64(Int(bitPattern: uint)))
		}
	}

	/// Creates a database value initialized to a signed integer value or null.
	public static func int64(_ value: Int64?) -> DatabaseValue {
		switch value {
		case .none:
			return .null
		case .some(let int64):
			return .integer(int64)
		}
	}

	/// Creates a database value initialized to a signed integer value or null.
	public static func uint64(_ value: UInt64?) -> DatabaseValue {
		switch value {
		case .none:
			return .null
		case .some(let uint64):
			return .integer(Int64(bitPattern: uint64))
		}
	}
}

extension DatabaseValue {
	/// Creates and returns  a database value containing a floating-point value.
	public static func float(_ value: Float) -> DatabaseValue {
		.real(Double(value))
	}

	/// Creates and returns  a database value containing a floating-point value.
	public static func double(_ value: Double) -> DatabaseValue {
		.real(value)
	}
}

extension DatabaseValue {
	/// Creates a database value initialized to a floating-point value or null.
	public static func float(_ value: Float?) -> DatabaseValue {
		switch value {
		case .none:
			return .null
		case .some(let float):
			return .real(Double(float))
		}
	}

	/// Creates a database value initialized to a floating-point value or null.
	public static func double(_ value: Double?) -> DatabaseValue {
		switch value {
		case .none:
			return .null
		case .some(let double):
			return .real(double)
		}
	}
}

extension DatabaseValue {
	/// Creates and returns  a database value containing a signed integer representation of a boolean value.
	/// - note: True is represented as 1 while false is represented as 0.
	public static func bool(_ value: Bool) -> DatabaseValue {
		.integer(value ? 1 : 0)
	}
}

extension DatabaseValue {
	/// Creates and returns  a database value containing a signed integer representation of a boolean value or null.
	/// - note: True is represented as 1 while false is represented as 0.
	public static func bool(_ value: Bool?) -> DatabaseValue {
		switch value {
		case .none:
			return .null
		case .some(let bool):
			return .integer(bool ? 1 : 0)
		}
	}
}

extension DatabaseValue {
	/// Creates and returns  a database value containing a text representation of a UUID.
	/// - note: The database value contains a lower case UUID string.
	public static func uuidString(_ value: UUID) -> DatabaseValue {
		.text(value.uuidString.lowercased())
	}

	/// Creates and returns  a database value containing a BLOB representation of a UUID.
	/// - note: The database value contains a 16-byte `uuid_t`.
	public static func uuidBytes(_ value: UUID) -> DatabaseValue {
		let b = withUnsafeBytes(of: value.uuid) {
			Data($0)
		}
		return .blob(b)
	}
}

extension DatabaseValue {
	/// Creates and returns  a database value containing a text representation of a URL.
	public static func urlString(_ value: URL) -> DatabaseValue {
		.text(value.absoluteString)
	}
}

extension DatabaseValue {
	/// Creates and returns  a database value containing a floating-point representation of a date.
	/// - note: The database value contains the number of seconds relative to 00:00:00 UTC on 1 January 1970.
	public static func timeIntervalSince1970(_ value: Date) -> DatabaseValue {
		.real(value.timeIntervalSince1970)
	}

	/// Creates and returns  a database value containing a floating-point representation of a date.
	/// - note: The database value contains the number of seconds relative to 00:00:00 UTC on 1 January 2001.
	public static func timeIntervalSinceReferenceDate(_ value: Date) -> DatabaseValue {
		.real(value.timeIntervalSinceReferenceDate)
	}

	/// Creates and returns  a database value containing a text representation of a date.
	/// - parameter formatter: The formatter to use to generate the ISO 8601 date representation.
	public static func iso8601DateString(_ value: Date, _ formatter: ISO8601DateFormatter = ISO8601DateFormatter()) -> DatabaseValue {
		.text(formatter.string(from: value))
	}
}

extension DatabaseValue {
	/// Creates and returns  a database value containing encoded JSON data.
	/// - parameter encoder: The encoder to use to generate the encoded JSON data.
	public static func json<T>(_ value: T, _ encoder: JSONEncoder = JSONEncoder()) throws -> DatabaseValue where T: Encodable {
		try .blob(encoder.encode(value))
	}
}

extension DatabaseValue {
	/// Creates and returns  a database value containing a signed integer or floating-point value.
	public static func number(_ value: NSNumber) -> DatabaseValue {
		switch CFNumberGetType(value as CFNumber) {
		case .sInt8Type, .sInt16Type, .sInt32Type, .charType, .shortType, .intType,
				.sInt64Type, .longType, .longLongType, .cfIndexType, .nsIntegerType:
			return .integer(value.int64Value)
		case .float32Type, .float64Type, .floatType, .doubleType, .cgFloatType:
			return .real(value.doubleValue)
		@unknown default:
			fatalError("Unexpected CFNumber type")
		}
	}
}

extension DatabaseValue {
	/// Creates and returns  a database value containing a keyed archive.
	public static func keyedArchive<T>(_ value: T) throws -> DatabaseValue where T: NSObject, T: NSCoding {
		let b = try NSKeyedArchiver.archivedData(withRootObject: value, requiringSecureCoding: true)
		return .blob(b)
	}
}
