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
