//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

extension Statement {
	/// Binds a BLOB filled with zeroes to the SQL parameter at `index`.
	///
	/// The BLOB's contents may be updated using [incremental BLOB I/O](https://sqlite.org/c3ref/blob_open.html).
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - requires: `length >= 0`
	///
	/// - parameter index: The index of the SQL parameter to bind
	/// - parameter length: The desired length of the BLOB in bytes
	///
	/// - throws: An error if the BLOB couldn't be bound
	public func bindZeroBLOB(toParameter index: Int, length: Int) throws {
		guard sqlite3_bind_zeroblob(preparedStatement, Int32(index), Int32(length)) == SQLITE_OK else {
			throw SQLiteError(fromDatabaseConnection: database.databaseConnection)
		}
	}

	/// Binds a BLOB filled with zeroes to the SQL parameter `name`.
	///
	/// The BLOB's contents may be updated using [incremental BLOB I/O](https://sqlite.org/c3ref/blob_open.html).
	///
	/// - requires: `length >= 0`.
	///
	/// - parameter name: The name of the SQL parameter to bind.
	/// - parameter length: The desired length of the BLOB in bytes.
	///
	/// - throws: An error if the SQL parameter `name` doesn't exist or the BLOB couldn't be bound.
	public func bindZeroBLOB(toParameter name: String, length: Int) throws {
		try bindZeroBLOB(toParameter: indexOfParameter(name), length: length)
	}
}
