//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation

/// A struct responsible for converting a result row to `T`.
///
/// For example, if `Person` is defined as:
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
///         let firstName = try row.text(forColumn: 0)
///         let lastName = try row.text(forColumn: 1)
///         return Person(firstName: firstName, lastName: lastName)
///     }
/// }
///  ```
public struct RowConverter<T> {
	/// Converts `row` to `T` and returns the result.
	///
	/// - parameter row: A `Row` object.
	///
	/// - throws: An error if the type conversion could not be accomplished.
	public let convert: (_ row: Row) throws -> T
}

extension Connection {
	/// Returns all rows in `table` converted to `type`.
	///
	/// This is equivalent to the SQL `SELECT * FROM "`*table*`"`.
	///
	/// - parameter type: The type of object to create from the row.
	/// - parameter converter: A `RowConverter` object to use for converting result rows to `type`.
	/// - parameter table: The name of the desired table.
	///
	/// - throws: An error if the SQL could not be compiled or executed, or if initialization fails.
	///
	/// - returns: All rows as `type`.
	public func all<T>(as type: T.Type = T.self, _ converter: RowConverter<T>, from table: String) throws -> [T] {
		let statement = try prepare(sql: "SELECT * FROM \"\(table)\";")
		var results = [T]()
		try statement.results { row in
			try results.append(converter.convert(row))
		}
		return results
	}

	/// Returns the first row in `table` converted to `type`.
	///
	/// This is equivalent to the SQL `SELECT * FROM "`*table*`" LIMIT 1`.
	///
	/// - parameter type: The type of object to create from the row.
	/// - parameter converter: A `RowConverter` object to use for converting result rows to `type`.
	/// - parameter table: The name of the desired table.
	///
	/// - throws: An error if the SQL could not be compiled or executed, or if initialization fails.
	///
	/// - returns: The first row as `type`.
	public func first<T>(as type: T.Type = T.self, _ converter: RowConverter<T>, from table: String) throws -> T? {
		let statement = try prepare(sql: "SELECT * FROM \"\(table)\" LIMIT 1;")
		guard let row = try statement.step() else {
			return nil
		}
		return try converter.convert(row)
	}

	/// Returns the rows in `table` matching `expression` converted to `type`.
	///
	/// This is equivalent to the SQL `SELECT * FROM "`*table*`" WHERE` *expression*.
	///
	/// - parameter type: The type of object to create from the row.
	/// - parameter converter: A `RowConverter` object to use for converting result rows to `type`.
	/// - parameter table: The name of the desired table.
	/// - parameter expression: An SQL expression defining the scope of the result rows.
	/// - parameter values: A collection of values to bind to SQL parameters.
	///
	/// - throws: An error if the SQL could not be compiled or executed, or if initialization fails.
	///
	/// - returns: The matching rows as `type`.
	///
	/// - seealso: [expr](http://sqlite.org/syntax/expr.html)
	public func find<T, C: Collection>(as type: T.Type = T.self, _ converter: RowConverter<T>, from table: String, `where` expression: String, parameters: C) throws -> [T] where C.Element == SQLParameter {
		let statement = try prepare(sql: "SELECT * FROM \"\(table)\" WHERE \(expression);")
		try statement.bind(parameters)
		var results = [T]()
		try statement.results { row in
			try results.append(converter.convert(row))
		}
		return results
	}

	/// Returns the rows in `table` matching `expression` converted to `type`.
	///
	/// This is equivalent to `SELECT * FROM "`*table*`" WHERE` *expression*.
	///
	/// - parameter type: The type of object to create from the row.
	/// - parameter converter: A `RowConverter` object to use for converting result rows to `type`.
	/// - parameter table: The name of the desired table.
	/// - parameter expression: An SQL expression defining the scope of the result rows.
	/// - parameter parameters: A collection of name and value pairs to bind to SQL parameters.
	///
	/// - throws: An error if the SQL could not be compiled or executed, or if initialization fails.
	///
	/// - returns: The matching rows as `type`.
	///
	/// - seealso: [expr](http://sqlite.org/syntax/expr.html)
	public func find<T, C: Collection>(as type: T.Type = T.self, _ converter: RowConverter<T>, from table: String, `where` expression: String, parameters: C) throws -> [T] where C.Element == (key: String, value: SQLParameter) {
		let statement = try prepare(sql: "SELECT * FROM \"\(table)\" WHERE \(expression);")
		try statement.bind(parameters)
		var results = [T]()
		try statement.results { row in
			try results.append(converter.convert(row))
		}
		return results
	}
}

extension Connection {
	/// Returns the rows in `table` matching `expression` converted to `type`.
	///
	/// This is equivalent to the SQL `SELECT * FROM "`*table*`" WHERE` *expression*.
	///
	/// - parameter type: The type of object to create from the row.
	/// - parameter converter: A `RowConverter` object to use for converting result rows to `type`.
	/// - parameter table: The name of the desired table.
	/// - parameter expression: An SQL expression defining the scope of the result rows.
	/// - parameter values: A collection of values to bind to SQL parameters.
	///
	/// - throws: An error if the SQL could not be compiled or executed, or if initialization fails.
	///
	/// - returns: The matching rows as `type`.
	///
	/// - seealso: [expr](http://sqlite.org/syntax/expr.html)
	public func find<T>(as type: T.Type = T.self, _ converter: RowConverter<T>, from table: String, `where` expression: String, parameters: SQLParameter...) throws -> [T] {
		try find(as: type, converter, from: table, where: expression, parameters: parameters)
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
