//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

/// An `Error` supplying an SQLite error code and description.
public struct SQLiteError: Error {
	/// A result code specifying the error.
	///
	/// - seealso: [Result and Error Codes](https://www.sqlite.org/rescode.html)
	public let code: Int32

	/// A more detailed description of the error's cause.
	public let details: String?

	/// Creates an error with the given SQLite error code and details.
	///
	/// - precondition: `code` is not equal to `SQLITE_OK`, `SQLITE_ROW`, or `SQLITE_DONE`.
	///
	/// - parameter code: An SQLite error code.
	/// - parameter details: A description of the error's cause.
	public init(code: Int32, details: String?) {
		precondition(code & 0xff != SQLITE_OK)
		precondition(code != SQLITE_ROW)
		precondition(code != SQLITE_DONE)
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
	/// Creates an error with the given code.
	///
	/// The description is obtained using `sqlite3_errstr(code)`.
	///
	/// - parameter code: An SQLite error code.
	public init(code: Int32) {
		self.init(code: code, details: String(cString: sqlite3_errstr(code)))
	}

	/// Creates an error with result code and description obtained from `db`.
	///
	/// The error code is obtained using `sqlite3_extended_errcode(db)`.
	/// The description is obtained using `sqlite3_errmsg(db)`.
	///
	/// - parameter db: An `sqlite3 *` database connection handle.
	public init(fromDatabaseConnection db: SQLiteDatabaseConnection) {
		self.init(code: sqlite3_extended_errcode(db), details: String(cString: sqlite3_errmsg(db)))
	}

	/// Creates an error with result code and description obtained from `stmt`.
	///
	/// The error code is obtained using `sqlite3_extended_errcode(sqlite3_db_handle(stmt))`.
	/// The description is obtained using `sqlite3_errmsg(sqlite3_db_handle(stmt))`.
	///
	/// - parameter stmt: An `sqlite3_stmt *` object.
	public init(fromPreparedStatement stmt: SQLitePreparedStatement) {
		self.init(fromDatabaseConnection: sqlite3_db_handle(stmt))
	}
}

extension SQLiteError: CustomStringConvertible {
	public var description: String {
		if let details = details {
			return "\(code): \(details)"
		} else {
			return "\(code)"
		}
	}
}
