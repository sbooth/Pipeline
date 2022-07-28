//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

/// An error supplying a message, an SQLite error code, and description.
public struct SQLiteError: Error {
	/// A brief message describing the circumstances leading to the error.
	public let message: String

	/// A result code specifying the error.
	///
	/// - seealso: [Result and Error Codes](https://www.sqlite.org/rescode.html)
	public let code: Int32

	/// A more detailed description of the error's cause.
	public let details: String?

	/// Creates an error with the given message, SQLite error code, and details.
	///
	/// - precondition: `code` is not equal to `SQLITE_OK`, `SQLITE_ROW`, or `SQLITE_DONE`.
	///
	/// - parameter message: A brief message describing the circumstances leading to the error.
	/// - parameter code: An SQLite error code.
	/// - parameter details: A description of the error's cause.
	public init(message: String, code: Int32, details: String?) {
		precondition(code & 0xff != SQLITE_OK)
		precondition(code != SQLITE_ROW)
		precondition(code != SQLITE_DONE)
		self.message = message
		self.code = code
		self.details = details
	}
}

extension SQLiteError {
	/// The primary error code.
	public var primaryCode: Int32 {
		code & 0xff
	}
	/// The extended error code.
	public var extendedCode: Int32 {
		code >> 8
	}
}

extension SQLiteError {
	/// Creates an error with the given message and code.
	///
	/// The description is obtained using `sqlite3_errstr(code)`.
	///
	/// - parameter message: A brief message describing the circumstances leading to the error.
	/// - parameter code: An SQLite error code
	public init(_ message: String, code: Int32 = SQLITE_ERROR) {
		self.init(message: message, code: code, details: String(cString: sqlite3_errstr(code)))
	}

	/// Creates an error with the given message, with result code and description obtained from `preparedStatement`.
	///
	/// The error code is obtained using `sqlite3_extended_errcode(sqlite3_db_handle(preparedStatement))`.
	/// The description is obtained using `sqlite3_errmsg(sqlite3_db_handle(preparedStatement))`.
	///
	/// - parameter message: A brief message describing the circumstances leading to the error.
	/// - parameter preparedStatement: An `sqlite3_stmt *` object.
	init(_ message: String, takingErrorCodeFromPreparedStatement preparedStatement: SQLitePreparedStatement) {
		self.init(message, takingErrorCodeFromDatabaseConnection: sqlite3_db_handle(preparedStatement))
	}

	/// Creates an error with the given message, with result code and description obtained from `databaseConnection`.
	///
	/// The error code is obtained using `sqlite3_extended_errcode(databaseConnection)`.
	/// The description is obtained using `sqlite3_errmsg(databaseConnection)`.
	///
	/// - parameter message: A brief message describing the circumstances leading to the error.
	/// - parameter databaseConnection: An `sqlite3 *` database connection handle.
	init(_ message: String, takingErrorCodeFromDatabaseConnection databaseConnection: SQLiteDatabaseConnection) {
		self.init(message: message, code: sqlite3_extended_errcode(databaseConnection), details: String(cString: sqlite3_errmsg(databaseConnection)))
	}
}

extension SQLiteError: CustomStringConvertible {
	public var description: String {
		if let details = details {
			return "\(message) [\(code)]: \(details)"
		} else {
			return "\(message) [\(code)]"
		}
	}
}

extension SQLiteError: LocalizedError {
	public var errorDescription: String? {
		return message
	}

	public var failureReason: String? {
		return details
	}
}
