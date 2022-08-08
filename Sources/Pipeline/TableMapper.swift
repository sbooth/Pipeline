//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation

/// A struct responsible for converting result rows from a specific table to `T`.
///
/// If a table `person` is defined as
///
/// ```
/// CREATE TABLE person(first_name TEXT, last_name TEXT);
/// ```
///
/// and a corresponding `Person` struct and  associated row converter are defined as
///
/// ```swift
/// struct Person {
///     let firstName: String
///     let lastName: String
/// }
///
/// extension RowConverter where T == Person {
///     public static let person = RowConverter { row in
///         let firstName = try row.text(at: 0)
///         let lastName = try row.text(at: 1)
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
///
/// The table mapper could be used from a database connection as:
///
/// ```swift
/// let someone = try connection.first(.person)
/// ```
public struct TableMapper<T> {
	/// The name of the table.
	public let table: String
	/// A row converter for converting result rows to `T`.
	public let converter: RowConverter<T>

	/// Creates a new table mapper.
	///
	/// - parameter name: The name of the table.
	/// - parameter converter: A row converter object.
	public init(table: String, converter: RowConverter<T>) {
		self.table = table
		self.converter = converter
	}
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
	public func all<T>(as type: T.Type = T.self, _ mapper: TableMapper<T>) throws -> [T] {
		try query(as: type, mapper.converter, sql: "SELECT * FROM \"\(mapper.table)\";")
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
	public func first<T>(as type: T.Type = T.self, _ mapper: TableMapper<T>) throws -> T? {
		try query(as: type, mapper.converter, sql: "SELECT * FROM \"\(mapper.table)\" LIMT 1;").first
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
	public func find<T, C: Collection>(as type: T.Type = T.self, _ mapper: TableMapper<T>, `where` expression: String, parameters: C) throws -> [T] where C.Element == SQLParameter {
		try query(as: type, mapper.converter, sql: "SELECT * FROM \"\(mapper.table)\" WHERE \(expression);", parameters: parameters)
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
	public func find<T, C: Collection>(as type: T.Type = T.self, _ mapper: TableMapper<T>, `where` expression: String, parameters: C) throws -> [T] where C.Element == (key: String, value: SQLParameter) {
		try query(as: type, mapper.converter, sql: "SELECT * FROM \"\(mapper.table)\" WHERE \(expression);", parameters: parameters)
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
	public func find<T>(as type: T.Type = T.self, _ mapper: TableMapper<T>, `where` expression: String, parameters: SQLParameter...) throws -> [T] {
		try find(as: type, mapper, where: expression, parameters: parameters)
	}
}
