//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

/// An `sqlite3_blob *` object.
///
/// - seealso: [A Handle To An Open BLOB](https://sqlite.org/c3ref/blob.html)
public typealias SQLiteBLOB = OpaquePointer

/// A BLOB supporting incremental I/O.
///
/// - seealso: [Open A BLOB For Incremental I/O](https://sqlite.org/c3ref/blob_open.html)
public final class BLOB {
	/// The owning database.
	public let database: Database

	/// The underlying `sqlite3_blob *` object.
	let blob: SQLiteBLOB

	/// Initializes a new BLOB for incremental I/O.
	///
	/// - note: This opens the BLOB that would be selected by `SELECT column FROM schema.table WHERE rowid = row;`.
	///
	/// - parameter database: The owning database.
	/// - parameter schema: The symbolic name of the database such as `main` or `temp`.
	/// - parameter table: The name of the desired table in `schema`.
	/// - parameter column: The name of the desired column in `table`.
	/// - parameter row: The desired rowid.
	/// - parameter readOnly: Whetherif the BLOB should be opened read-only.
	///
	/// - throws: An error if the BLOB could not be opened.
	init(database: Database, schema: String, table: String, column: String, row: Int64, readOnly: Bool) throws {
		self.database = database
		var blob: SQLiteBLOB? = nil
		guard sqlite3_blob_open(database.databaseConnection, schema, table, column, row, readOnly ? 0 : 1, &blob) == SQLITE_OK else {
			throw SQLiteError("Error opening BLOB for schema \"\(schema)\"", takingErrorCodeFromDatabaseConnection: database.databaseConnection)
		}
		precondition(blob != nil)
		self.blob = blob.unsafelyUnwrapped
	}

	deinit {
		_ = sqlite3_blob_close(blob);
	}

	/// The length of the BLOB in bytes.
	public var length: Int {
		let bytes = sqlite3_blob_bytes(blob)
		return Int(bytes)
	}

	/// Reads bytes from a BLOB into a buffer.
	///
	/// - requires: `length >= 0`.
	/// - requires: `offset >= 0`.
	/// - requires: `length + offset <= self.length`.
	///
	/// - parameter buffer: A buffer to hold the bytes.
	/// - parameter length: The number of bytes to read.
	/// - parameter offset: The starting offset in the BLOB to start reading.
	///
	/// - throws: An error if unsufficient bytes are available or a read error occurs.
	public func read(into buffer: UnsafeMutableRawPointer, length: Int, readOffset offset: Int) throws {
		guard sqlite3_blob_read(blob, buffer, Int32(length), Int32(offset)) == SQLITE_OK else {
			throw SQLiteError("Error reading \(length) bytes from BLOB at offset \(offset)", takingErrorCodeFromDatabaseConnection: database.databaseConnection)
		}
	}

	/// Writes bytes to a BLOB from a buffer.
	///
	/// - requires: `length >= 0`.
	/// - requires: `offset >= 0`.
	/// - requires: `length + offset <= self.length`.
	///
	/// - note: This function may only modify the contents of the BLOB; it is not possible to increase the size of a BLOB.
	///
	/// - parameter buffer: A buffer holding the bytes.
	/// - parameter length: The number of bytes to write.
	/// - parameter offset: The starting offset in the BLOB to start writing.
	///
	/// - throws: An error if unsufficient bytes are available or a read error occurs.
	public func write(from buffer: UnsafeRawPointer, length: Int, writeOffset offset: Int) throws {
		let result = sqlite3_blob_write(blob, buffer, Int32(length), Int32(offset))
		guard result == SQLITE_OK else {
			throw SQLiteError("Error writing \(length) bytes to BLOB at offset \(offset)", code: result)
		}
	}

	/// Moves the BLOB to another row in the same database table.
	///
	/// - parameter rowid: The rowid of the desired row.
	///
	/// - throws: An error if the BLOB could not be moved to the specfied row.
	public func reopen(rowid: Int64) throws {
		guard sqlite3_blob_reopen(blob, rowid) == SQLITE_OK else {
			throw SQLiteError("Error reopening BLOB for row \(rowid)", takingErrorCodeFromDatabaseConnection: database.databaseConnection)
		}
	}
}

extension Database {
	/// Opens and returns a BLOB for incremental I/O.
	///
	/// - note: This is the BLOB that would be selected by `SELECT column FROM schema.table WHERE rowid = row;`.
	///
	/// - parameter schema: The symbolic name of the database such as `main` or `temp`.
	/// - parameter table: The name of the desired table in `schema`.
	/// - parameter column: The name of the desired column in `table`.
	/// - parameter row: The desired rowid.
	/// - parameter readOnly: Whetherif the BLOB should be opened read-only.
	///
	/// - throws: An error if the BLOB could not be created.
	///
	/// - returns: An initialized `BLOB` for incremental reading.
	public func openBLOB(schema: String, table: String, column: String, row: Int64, readOny: Bool) throws -> BLOB {
		try BLOB(database: self, schema: schema, table: table, column: column, row: row, readOnly: readOny)
	}
}
