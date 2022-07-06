//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import Combine

/// A decoder for `Row` for  Combine's `.decode(type:decoder:)` operator.
public class RowDecoder: TopLevelDecoder {
	/// A method  used to translate `DatabaseValue` into `Date`.
	public enum DateDecodingMethod {
		/// Defer to `Date` for decoding.
		case deferredToDate
		/// Decode the date as a floating-point number containing the interval between the date and 00:00:00 UTC on 1 January 1970.
		case timeIntervalSince1970
		/// Decode the date as a floating-point number containing the interval between the date and 00:00:00 UTC on 1 January 2001.
		case timeIntervalSinceReferenceDate
		/// Decode the date as ISO-8601 formatted text.
		case iso8601(ISO8601DateFormatter.Options)
		/// Decode the date as text parsed by the given formatter.
		case formatted(DateFormatter)
		/// Decode the date using the given closure.
		case custom((_ value: DatabaseValue) throws -> Date)
	}

	/// The method  used to translate `DatabaseValue` into `Date`.
	var dateDecodingMethod: DateDecodingMethod = .deferredToDate

	/// Currently not used.
	open var userInfo: [CodingUserInfoKey: Any] = [:]

	fileprivate struct Options {
		let dateDecodingStrategy: DateDecodingMethod
		let userInfo: [CodingUserInfoKey: Any]
	}

	fileprivate var options: Options {
		Options(dateDecodingStrategy: dateDecodingMethod, userInfo: userInfo)
	}

	/// Decodes and returns an object of `type` using the column values from `row`.
	///
	/// - parameter type: The type of object to decode.
	/// - parameter row: The database row used to populate.
	///
	/// - throws: An error if decoding was unsuccessful.
	///
	/// - returns: An instance of `type`.
	public func decode<T>(_ type: T.Type, from row: Row) throws -> T where T : Decodable {
		let decoder = RowDecoderGuts(payload: .row(row), codingPath: [], userInfo: userInfo, options: options)
		return try T(from: decoder)
	}
}

private struct RowDecoderGuts {
	enum Payload {
		case row(Row)
		case value(DatabaseValue)
	}
	let payload: Payload
	let codingPath: [CodingKey]
	let userInfo: [CodingUserInfoKey: Any]
	let options: RowDecoder.Options
	var iso8601DateFormatter: ISO8601DateFormatter?
}

extension RowDecoderGuts {
	init(payload: Payload, codingPath: [CodingKey], userInfo: [CodingUserInfoKey: Any], options: RowDecoder.Options) {
		self.payload = payload
		self.codingPath = codingPath
		self.userInfo = userInfo
		self.options = options
		if case let .iso8601(options) = options.dateDecodingStrategy {
			iso8601DateFormatter = ISO8601DateFormatter()
			iso8601DateFormatter!.formatOptions = options
		}
	}
}

extension RowDecoderGuts: Decoder {
	func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> where Key : CodingKey {
		guard case let .row(row) = payload else {
			throw DecodingError.typeMismatch(Row.self, DecodingError.Context(codingPath: codingPath, debugDescription: "Expected to decode Row but found DatabaseValue."))
		}
		let container = KeyedContainer<Key>(values: try row.valueDictionary(), decoder: self, codingPath: codingPath)
		return KeyedDecodingContainer(container)
	}

	func unkeyedContainer() throws -> UnkeyedDecodingContainer {
		guard case let .row(row) = payload else {
			throw DecodingError.typeMismatch(Row.self, DecodingError.Context(codingPath: codingPath, debugDescription: "Expected to decode Row but found DatabaseValue."))
		}
		return UnkeyedContainer(values: try row.values(), decoder: self, codingPath: codingPath)
	}

	func singleValueContainer() throws -> SingleValueDecodingContainer {
		guard case let .value(value) = payload else {
			throw DecodingError.typeMismatch(DatabaseValue.self, DecodingError.Context(codingPath: codingPath, debugDescription: "Expected to decode DatabaseValue but found Row."))
		}
		return SingleValueContainer(value: value, decoder: self, codingPath: codingPath)
	}
}

extension RowDecoderGuts {
	func decode<T>(as type: T.Type) throws -> T where T : Decodable {
		guard case let .value(value) = payload else {
			throw DecodingError.typeMismatch(DatabaseValue.self, DecodingError.Context(codingPath: codingPath, debugDescription: "Expected to decode DatabaseValue but found Row."))
		}
		return try decode(value, as: type)
	}

	func decode<T>(_ value: DatabaseValue, as type: T.Type) throws -> T where T : Decodable {
		if type == Date.self {
			return try decodeDate(value) as! T
		} else if type == URL.self {
			return try decodeURL(value) as! T
		} else {
			return try T(from: self)
		}
	}

	func decodeFixedWidthInteger<T>(_ value: DatabaseValue) throws -> T where T: FixedWidthInteger {
		guard case let .integer(i) = value else {
			throw DecodingError.typeMismatch(T.self, DecodingError.Context(codingPath: codingPath, debugDescription: "Database value type is not integer."))
		}
		return T(i)
	}

	func decodeFloatingPoint<T>(_ value: DatabaseValue) throws -> T where T: BinaryFloatingPoint {
		guard case let .real(r) = value else {
			throw DecodingError.typeMismatch(T.self, DecodingError.Context(codingPath: codingPath, debugDescription: "Database value type is not float."))
		}
		return T(r)
	}

	private func decodeDate(_ value: DatabaseValue) throws -> Date {
		switch options.dateDecodingStrategy {
		case .deferredToDate:
			return try Date(from: self)

		case .timeIntervalSince1970:
			guard case let .real(r) = value else {
				throw DecodingError.typeMismatch(Date.self, DecodingError.Context(codingPath: codingPath, debugDescription: "Database value type is not float."))
			}
			return Date(timeIntervalSince1970: r)

		case .timeIntervalSinceReferenceDate:
			guard case let .real(r) = value else {
				throw DecodingError.typeMismatch(Date.self, DecodingError.Context(codingPath: codingPath, debugDescription: "Database value type is not float."))
			}
			return Date(timeIntervalSinceReferenceDate: r)

		case .iso8601:
			guard case let .text(t) = value else {
				throw DecodingError.typeMismatch(Date.self, DecodingError.Context(codingPath: codingPath, debugDescription: "Database value type is not text."))
			}
			precondition(iso8601DateFormatter != nil)
			guard let date = iso8601DateFormatter!.date(from: t) else {
				throw DecodingError.dataCorrupted(DecodingError.Context(codingPath: codingPath, debugDescription: "String \"\(t)\" isn't a valid ISO8601 date."))
			}
			return date

		case .formatted(let formatter):
			guard case let .text(t) = value else {
				throw DecodingError.typeMismatch(Date.self, DecodingError.Context(codingPath: codingPath, debugDescription: "Database value type is not text."))
			}
			guard let date = formatter.date(from: t) else {
				throw DecodingError.dataCorrupted(DecodingError.Context(codingPath: codingPath, debugDescription: "String \"\(t)\" doesn't match the expected date format."))
			}
			return date

		case .custom(let closure):
			return try closure(value)
		}
	}

	private func decodeURL(_ value: DatabaseValue) throws -> URL {
		guard case let .text(t) = value else {
			throw DecodingError.typeMismatch(Date.self, DecodingError.Context(codingPath: codingPath, debugDescription: "Database value type is not text."))
		}
		guard let url = URL(string: t) else {
			throw DecodingError.dataCorrupted(DecodingError.Context(codingPath: codingPath, debugDescription: "Invalid URL string."))
		}
		return url
	}
}

private struct KeyedContainer<K: CodingKey>: KeyedDecodingContainerProtocol {
	let values: [String: DatabaseValue]
	let decoder: RowDecoderGuts
	let codingPath: [CodingKey]

	var allKeys: [Key] {
		values.keys.compactMap { Key(stringValue: $0) }
	}

	func contains(_ key: K) -> Bool {
		values[key.stringValue] != nil
	}

	func decodeNil(forKey key: K) throws -> Bool {
		let value = try valueForKey(key)
		if case .null = value {
			return true
		} else {
			return false
		}
	}

	func decode(_ type: Bool.Type, forKey key: K) throws -> Bool {
		let value = try valueForKey(key)
		guard case let .integer(i) = value else {
			throw DecodingError.typeMismatch(type, DecodingError.Context(codingPath: codingPath, debugDescription: "Column \"\(key)\" type is not integer."))
		}
		return i != 0
	}

	func decode(_ type: String.Type, forKey key: K) throws -> String {
		let value = try valueForKey(key)
		guard case let .text(s) = value else {
			throw DecodingError.typeMismatch(type, DecodingError.Context(codingPath: codingPath, debugDescription: "Column \"\(key)\" type is not text."))
		}
		return s
	}

	func decode(_ type: Double.Type, forKey key: K) throws -> Double {
		return try decodeFloatingPointForKey(key)
	}

	func decode(_ type: Float.Type, forKey key: K) throws -> Float {
		return try decodeFloatingPointForKey(key)
	}

	func decode(_ type: Int.Type, forKey key: K) throws -> Int {
		return try decodeFixedWidthIntegerForKey(key)
	}

	func decode(_ type: Int8.Type, forKey key: K) throws -> Int8 {
		return try decodeFixedWidthIntegerForKey(key)
	}

	func decode(_ type: Int16.Type, forKey key: K) throws -> Int16 {
		return try decodeFixedWidthIntegerForKey(key)
	}

	func decode(_ type: Int32.Type, forKey key: K) throws -> Int32 {
		return try decodeFixedWidthIntegerForKey(key)
	}

	func decode(_ type: Int64.Type, forKey key: K) throws -> Int64 {
		return try decodeFixedWidthIntegerForKey(key)
	}

	func decode(_ type: UInt.Type, forKey key: K) throws -> UInt {
		return try decodeFixedWidthIntegerForKey(key)
	}

	func decode(_ type: UInt8.Type, forKey key: K) throws -> UInt8 {
		return try decodeFixedWidthIntegerForKey(key)
	}

	func decode(_ type: UInt16.Type, forKey key: K) throws -> UInt16 {
		return try decodeFixedWidthIntegerForKey(key)
	}

	func decode(_ type: UInt32.Type, forKey key: K) throws -> UInt32 {
		return try decodeFixedWidthIntegerForKey(key)
	}

	func decode(_ type: UInt64.Type, forKey key: K) throws -> UInt64 {
		return try decodeFixedWidthIntegerForKey(key)
	}

	func decode<T>(_ type: T.Type, forKey key: K) throws -> T where T : Decodable {
		let value = try valueForKey(key)
		let decoder = RowDecoderGuts(payload: .value(value), codingPath: codingPath.appending(key), userInfo: self.decoder.userInfo, options: self.decoder.options)
		return try decoder.decode(as: type)
	}

	func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type, forKey key: K) throws -> KeyedDecodingContainer<NestedKey> where NestedKey : CodingKey {
		fatalError("nestedContainer(keyedBy:) not implemented for KeyedContainer")
	}

	func nestedUnkeyedContainer(forKey key: K) throws -> UnkeyedDecodingContainer {
		fatalError("nestedUnkeyedContainer() not implemented for KeyedContainer")
	}

	func superDecoder() throws -> Decoder {
		fatalError("superDecoder() not implemented for KeyedContainer")
	}

	func superDecoder(forKey key: K) throws -> Decoder {
		fatalError("superDecoder(forKey:) not implemented for KeyedContainer")
	}

	private func valueForKey(_ key: K) throws -> DatabaseValue {
		guard let value = values[key.stringValue] else {
			throw DecodingError.keyNotFound(key, DecodingError.Context(codingPath: codingPath, debugDescription: "Column \"\(key)\" not found."))
		}
		return value
	}

	private func decodeFixedWidthIntegerForKey<T>(_ key: K) throws -> T where T: FixedWidthInteger {
		return try decoder.decodeFixedWidthInteger(try valueForKey(key))
	}

	private func decodeFloatingPointForKey<T>(_ key: K) throws -> T where T: BinaryFloatingPoint {
		return try decoder.decodeFloatingPoint(try valueForKey(key))
	}
}

private struct UnkeyedContainer: UnkeyedDecodingContainer {
	let values: [DatabaseValue]
	let decoder: RowDecoderGuts
	let codingPath: [CodingKey]
	var currentIndex: Int = 0

	var count: Int? {
		values.count
	}

	var isAtEnd: Bool {
		currentIndex == values.count
	}

	mutating func decodeNil() throws -> Bool {
		guard !isAtEnd else {
			throw DecodingError.valueNotFound(Never.self, DecodingError.Context(codingPath: codingPath, debugDescription: "Unkeyed container is at end."))
		}
		let value = values[currentIndex]
		if case .null = value {
			currentIndex += 1
			return true
		} else {
			return false
		}
	}

	mutating func decode(_ type: Bool.Type) throws -> Bool {
		guard !isAtEnd else {
			throw DecodingError.valueNotFound(Never.self, DecodingError.Context(codingPath: codingPath, debugDescription: "Unkeyed container is at end."))
		}
		let value = values[currentIndex]
		guard case let .integer(i) = value else {
			throw DecodingError.typeMismatch(type, DecodingError.Context(codingPath: codingPath, debugDescription: "Database value type is not integer."))
		}
		currentIndex += 1
		return i != 0
	}

	mutating func decode(_ type: String.Type) throws -> String {
		guard !isAtEnd else {
			throw DecodingError.valueNotFound(Never.self, DecodingError.Context(codingPath: codingPath, debugDescription: "Unkeyed container is at end."))
		}
		let value = values[currentIndex]
		guard case let .text(s) = value else {
			throw DecodingError.typeMismatch(type, DecodingError.Context(codingPath: codingPath, debugDescription: "Database value type is not text."))
		}
		currentIndex += 1
		return s

	}

	mutating func decode(_ type: Double.Type) throws -> Double {
		try decodeFloatingPoint()
	}

	mutating func decode(_ type: Float.Type) throws -> Float {
		try decodeFloatingPoint()
	}

	mutating func decode(_ type: Int.Type) throws -> Int {
		try decodeFixedWidthInteger()
	}

	mutating func decode(_ type: Int8.Type) throws -> Int8 {
		try decodeFixedWidthInteger()
	}

	mutating func decode(_ type: Int16.Type) throws -> Int16 {
		try decodeFixedWidthInteger()
	}

	mutating func decode(_ type: Int32.Type) throws -> Int32 {
		try decodeFixedWidthInteger()
	}

	mutating func decode(_ type: Int64.Type) throws -> Int64 {
		try decodeFixedWidthInteger()
	}

	mutating func decode(_ type: UInt.Type) throws -> UInt {
		try decodeFixedWidthInteger()
	}

	mutating func decode(_ type: UInt8.Type) throws -> UInt8 {
		try decodeFixedWidthInteger()
	}

	mutating func decode(_ type: UInt16.Type) throws -> UInt16 {
		try decodeFixedWidthInteger()
	}

	mutating func decode(_ type: UInt32.Type) throws -> UInt32 {
		try decodeFixedWidthInteger()
	}

	mutating func decode(_ type: UInt64.Type) throws -> UInt64 {
		try decodeFixedWidthInteger()
	}

	mutating func decode<T>(_ type: T.Type) throws -> T where T : Decodable {
		guard !isAtEnd else {
			throw DecodingError.valueNotFound(Never.self, DecodingError.Context(codingPath: codingPath, debugDescription: "Unkeyed container is at end."))
		}
		let value = values[currentIndex]
		let result: T = try decoder.decode(value, as: type)
		currentIndex += 1
		return result
	}

	mutating func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type) throws -> KeyedDecodingContainer<NestedKey> where NestedKey : CodingKey {
		fatalError("nestedContainer(keyedBy:) not implemented for UnkeyedContainer")
	}

	mutating func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
		fatalError("nestedUnkeyedContainer() not implemented for UnkeyedContainer")
	}

	mutating func superDecoder() throws -> Decoder {
		fatalError("superDecoder() not implemented for UnkeyedContainer")
	}

	private mutating func decodeFixedWidthInteger<T>() throws -> T where T: FixedWidthInteger {
		guard !isAtEnd else {
			throw DecodingError.valueNotFound(Never.self, DecodingError.Context(codingPath: codingPath, debugDescription: "Unkeyed container is at end."))
		}
		let value = values[currentIndex]
		let result: T = try decoder.decodeFixedWidthInteger(value)
		currentIndex += 1
		return result
	}

	private mutating func decodeFloatingPoint<T>() throws -> T where T: BinaryFloatingPoint {
		guard !isAtEnd else {
			throw DecodingError.valueNotFound(Never.self, DecodingError.Context(codingPath: codingPath, debugDescription: "Unkeyed container is at end."))
		}
		let value = values[currentIndex]
		let result: T = try decoder.decodeFloatingPoint(value)
		currentIndex += 1
		return result
	}
}

private struct SingleValueContainer: SingleValueDecodingContainer {
	let value: DatabaseValue
	let decoder: RowDecoderGuts
	let codingPath: [CodingKey]

	func decodeNil() -> Bool {
		if case .null = value {
			return true
		} else {
			return false
		}
	}

	func decode(_ type: Bool.Type) throws -> Bool {
		guard case let .integer(i) = value else {
			throw DecodingError.typeMismatch(type, DecodingError.Context(codingPath: codingPath, debugDescription: "Database value type is not integer."))
		}
		return i != 0
	}

	func decode(_ type: String.Type) throws -> String {
		guard case let .text(s) = value else {
			throw DecodingError.typeMismatch(type, DecodingError.Context(codingPath: codingPath, debugDescription: "Database value type is not text."))
		}
		return s
	}

	func decode(_ type: Double.Type) throws -> Double {
		try decodeFloatingPoint()
	}

	func decode(_ type: Float.Type) throws -> Float {
		try decodeFloatingPoint()
	}

	func decode(_ type: Int.Type) throws -> Int {
		try decodeFixedWidthInteger()
	}

	func decode(_ type: Int8.Type) throws -> Int8 {
		try decodeFixedWidthInteger()
	}

	func decode(_ type: Int16.Type) throws -> Int16 {
		try decodeFixedWidthInteger()
	}

	func decode(_ type: Int32.Type) throws -> Int32 {
		try decodeFixedWidthInteger()
	}

	func decode(_ type: Int64.Type) throws -> Int64 {
		try decodeFixedWidthInteger()
	}

	func decode(_ type: UInt.Type) throws -> UInt {
		try decodeFixedWidthInteger()
	}

	func decode(_ type: UInt8.Type) throws -> UInt8 {
		try decodeFixedWidthInteger()
	}

	func decode(_ type: UInt16.Type) throws -> UInt16 {
		return try decodeFixedWidthInteger()
	}

	func decode(_ type: UInt32.Type) throws -> UInt32 {
		try decodeFixedWidthInteger()
	}

	func decode(_ type: UInt64.Type) throws -> UInt64 {
		try decodeFixedWidthInteger()
	}

	func decode<T>(_ type: T.Type) throws -> T where T : Decodable {
		try decoder.decode(value, as: type)
	}

	private func decodeFixedWidthInteger<T>() throws -> T where T: FixedWidthInteger {
		try decoder.decodeFixedWidthInteger(value)
	}

	private func decodeFloatingPoint<T>() throws -> T where T: BinaryFloatingPoint {
		try decoder.decodeFloatingPoint(value)
	}
}

private extension RangeReplaceableCollection {
	/// Returns a new collection by adding `element` to the end of the collection.
	func appending(_ element: Element) -> Self {
		var mutable = Self(self)
		mutable.append(element)
		return mutable
	}
}
