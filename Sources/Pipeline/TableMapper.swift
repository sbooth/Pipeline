//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation

/// A struct responsible for converting result rows from a specific table to `T`.
///
/// For example, if `Person` and the associated row converter are defined as:
///
/// ```swift
/// struct Person {
///     let firstName: String
///     let lastName: String
/// }
///
/// extension RowConverter where T == Person {
///     public static let person = RowConverter { row in
///         let firstName = try row.text(forColumn: 0)
///         let lastName = try row.text(forColumn: 1)
///         return Person(firstName: firstName, lastName: lastName)
///     }
/// }
/// ```
///
/// An implementation of `TableMapper` for `Person` could be:
///
/// ```swift
/// extension TableMapper where T == Person {
///     public static let person = TableMapper(table: "person", converter: .person)
/// }
/// ```
struct TableMapper<T> {
	/// The name of the table.
	let table: String
	/// A row converter for converting result rows to `T`.
	let converter: RowConverter<T>
}

extension Connection {
	/// Returns all rows in `mapper.table` converted to `type`.
	///
	/// This is equivalent to the SQL `SELECT * FROM "`*mapper.table*`"`.
	///
	/// - parameter type: The type of object to create from each row.
	/// - parameter mapper: A `TableMapper` object.
	///
	/// - throws: An error if the SQL could not be compiled or executed, or if initialization fails.
	///
	/// - returns: All rows in `mapper.table` as `type`.
	func all<T>(as type: T.Type = T.self, _ mapper: TableMapper<T>) throws -> [T] {
		try all(as: type, mapper.converter, from: mapper.table)
	}

	/// Returns the first row in `mapper.table` converted to `type`.
	///
	/// This is equivalent to the SQL `SELECT * FROM "`*mapper.table*`" LIMIT 1`.
	///
	/// - parameter type: The type of object to create from each row.
	/// - parameter mapper: A `TableMapper` object.
	///
	/// - throws: An error if the SQL could not be compiled or executed, or if initialization fails.
	///
	/// - returns: The first row in `mapper.table` as `type`.
	func first<T>(as type: T.Type = T.self, _ mapper: TableMapper<T>) throws -> T? {
		try first(as: type, mapper.converter, from: mapper.table)
	}

	/// Returns the rows in `mapper.table` matching `expression` converted to `type`.
	///
	/// This is equivalent to the SQL `SELECT * FROM "`*mapper.table*`" WHERE` *expression*.
	///
	/// - parameter type: The type of object to create from each row.
	/// - parameter mapper: A `TableMapper` object.
	/// - parameter expression: An SQL expression defining the scope of the result rows.
	/// - parameter parameters: A collection of values to bind to SQL parameters.
	///
	/// - throws: An error if the SQL could not be compiled or executed, or if initialization fails.
	///
	/// - returns: The matching rows in `mapper.table` as `type`.
	///
	/// - seealso: [expr](http://sqlite.org/syntax/expr.html)
	func find<T, C: Collection>(as type: T.Type = T.self, _ mapper: TableMapper<T>, `where` expression: String, parameters: C) throws -> [T] where C.Element == SQLParameter {
		try find(as: type, mapper.converter, from: mapper.table, where: expression, parameters: parameters)
	}

	/// Returns the rows in `mapper.table` matching `expression` converted to `type`.
	///
	/// This is equivalent to the SQL `SELECT * FROM "`*mapper.table*`" WHERE` *expression*.
	///
	/// - parameter type: The type of object to create from each row.
	/// - parameter mapper: A `TableMapper` object.
	/// - parameter expression: An SQL expression defining the scope of the result rows.
	/// - parameter parameters: A collection of name and value pairs to bind to SQL parameters.
	///
	/// - throws: An error if the SQL could not be compiled or executed, or if initialization fails.
	///
	/// - returns: The matching rows in `mapper.table` as `type`.
	///
	/// - seealso: [expr](http://sqlite.org/syntax/expr.html)
	func find<T, C: Collection>(as type: T.Type = T.self, _ mapper: TableMapper<T>, `where` expression: String, parameters: C) throws -> [T] where C.Element == (key: String, value: SQLParameter) {
		try find(as: type, mapper.converter, from: mapper.table, where: expression, parameters: parameters)
	}
}

extension Connection {
	/// Returns the rows in `mapper.table` matching `expression` converted to `type`.
	///
	/// This is equivalent to the SQL `SELECT * FROM "`*mapper.table*`" WHERE` *expression*.
	///
	/// - parameter type: The type of object to create from each row.
	/// - parameter mapper: A `TableMapper` object.
	/// - parameter expression: An SQL expression defining the scope of the result rows.
	/// - parameter parameters: A collection of values to bind to SQL parameters.
	///
	/// - throws: An error if the SQL could not be compiled or executed, or if initialization fails.
	///
	/// - returns: The matching rows in `mapper.table` as `type`.
	///
	/// - seealso: [expr](http://sqlite.org/syntax/expr.html)
	func find<T>(as type: T.Type = T.self, _ mapper: TableMapper<T>, `where` expression: String, parameters: SQLParameter...) throws -> [T] {
		try find(as: type, mapper.converter, from: mapper.table, where: expression, parameters: parameters)
	}
}
