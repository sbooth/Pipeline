//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation

/// A struct responsible for converting a result row to `T`.
///
/// If a table `person` is defined as
///
/// ```
/// CREATE TABLE person(first_name TEXT, last_name TEXT);
/// ```
///
/// and a corresponding `Person` struct is defined as
///
/// ```swift
/// struct Person {
///     let firstName: String
///     let lastName: String
/// }
/// ```
///
/// An implementation of `RowConverter` for `Person` could be:
///
/// ```swift
/// extension RowConverter where T == Person {
///     public static let person = RowConverter { row in
///         let firstName = try row.text(at: 0)
///         let lastName = try row.text(at: 1)
///         return Person(firstName: firstName, lastName: lastName)
///     }
/// }
/// ```
///
/// The row converter could be used from a database connection as:
///
/// ```swift
/// let sql = "SELECT * FROM person LIMIT 1;"
/// let someone = try connection.query(.person, sql: sql).first
/// ```
public struct RowConverter<T> {
	/// Converts `row` to `T` and returns the result.
	///
	/// - parameter row: A `Row` object.
	///
	/// - throws: An error if the type conversion could not be accomplished.
	public let convert: (_ row: Row) throws -> T

	/// Creates a new row converter.
	///
	/// - parameter convert: A closure converting `row` to `T`.
	/// - parameter row: A `Row` object.
	public init(convert: @escaping (_ row: Row) throws -> T) {
		self.convert = convert
	}
}

extension Statement {
	/// Executes the statement, converts each result row to `type` using `converter`, and applies `block` to each resultant object.
	///
	/// - parameter type: The type of object to create from each row.
	/// - parameter converter: A `RowConverter` object to use for converting result rows to `type`.
	/// - parameter block: A closure applied to each resultant object.
	/// - parameter object: A resultant object.
	///
	/// - throws: Any error thrown in `block` or an error if the statement did not successfully run to completion or object initialization fails.
	public func results<T>(as type: T.Type = T.self, _ converter: RowConverter<T>, _ block: ((_ object: T) throws -> ())) throws {
		try results { row in
			try block(converter.convert(row))
		}
	}
}

extension Connection {
	/// Returns all result rows from `sql` converted to `type` using `converter`.
	///
	/// - parameter type: The type of object to create from each row.
	/// - parameter converter: A `RowConverter` object to use for converting result rows to `type`.
	/// - parameter sql: The SQL to execute.
	///
	/// - throws: An error if the SQL could not be compiled or executed, or if initialization fails.
	///
	/// - returns: The result rows converter to `type`.
	public func query<T>(as type: T.Type = T.self, _ converter: RowConverter<T>, sql: String) throws -> [T] {
		let statement = try prepare(sql: sql)
		var results = [T]()
		try statement.results(as: type, converter) { object in
			results.append(object)
		}
		return results
	}

	/// Returns all result rows from `sql` converted to `type` using `converter`.
	///
	/// - parameter type: The type of object to create from each row.
	/// - parameter converter: A `RowConverter` object to use for converting result rows to `type`.
	/// - parameter sql: The SQL to execute.
	/// - parameter parameters: A collection of values to bind to SQL parameters.
	///
	/// - throws: An error if the SQL could not be compiled or executed, or if initialization fails.
	///
	/// - returns: The result rows converter to `type`.
	public func query<T, C: Collection>(as type: T.Type = T.self, _ converter: RowConverter<T>, sql: String, parameters: C) throws -> [T] where C.Element == SQLParameter {
		let statement = try prepare(sql: sql)
		try statement.bind(parameters)
		var results = [T]()
		try statement.results(as: type, converter) { object in
			results.append(object)
		}
		return results
	}

	/// Returns all result rows from `sql` converted to `type` using `converter`.
	///
	/// - parameter type: The type of object to create from each row.
	/// - parameter converter: A `RowConverter` object to use for converting result rows to `type`.
	/// - parameter sql: The SQL to execute.
	/// - parameter parameters: A collection of name and value pairs to bind to SQL parameters.
	///
	/// - throws: An error if the SQL could not be compiled or executed, or if initialization fails.
	///
	/// - returns: The result rows converter to `type`.
	public func query<T, C: Collection>(as type: T.Type = T.self, _ converter: RowConverter<T>, sql: String, parameters: C) throws -> [T] where C.Element == (key: String, value: SQLParameter) {
		let statement = try prepare(sql: sql)
		try statement.bind(parameters)
		var results = [T]()
		try statement.results(as: type, converter) { object in
			results.append(object)
		}
		return results
	}
}

extension Connection {
	/// Returns all result rows from `sql` converted to `type` using `converter`.
	///
	/// - parameter type: The type of object to create from each row.
	/// - parameter converter: A `RowConverter` object to use for converting result rows to `type`.
	/// - parameter sql: The SQL to execute.
	/// - parameter parameters: A collection of values to bind to SQL parameters.
	///
	/// - throws: An error if the SQL could not be compiled or executed, or if initialization fails.
	///
	/// - returns: The result rows converter to `type`.
	public func query<T>(as type: T.Type = T.self, _ converter: RowConverter<T>, sql: String, parameters: SQLParameter...) throws -> [T] {
		try query(as: type, converter, sql: sql, parameters: parameters)
	}
}

#if canImport(Combine)

import Combine

extension Publisher {
	/// Returns a publisher mapping upstream result rows to objects of `type`.
	///
	/// - parameter type: The type of object to create from the row.
	/// - parameter converter: A `RowConverter` object to use for converting result rows to `type`.
	public func mapRows<T>(as type: T.Type = T.self, _ converter: RowConverter<T>) -> Publishers.TryMap<Self, T> where Output == Row {
//	public func mapRows<T>(as type: T.Type = T.self, _ converter: RowConverter<T>) -> AnyPublisher<T, Error> where Output == Row {
		return self
			.tryMap {
				try converter.convert($0)
			}
//			.eraseToAnyPublisher()
	}
}

#endif
