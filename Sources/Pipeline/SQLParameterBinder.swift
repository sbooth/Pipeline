//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

/// A struct responsible for binding a value not easily represented as a `DatabaseValue` object
/// to an SQL parameter in a `Statement` object.
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
