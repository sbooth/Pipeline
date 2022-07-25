//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

#if canImport(Combine)
import Combine
#endif

/// An `sqlite3 *` object.
///
/// - seealso: [SQLite Database Connection Handle](https://sqlite.org/c3ref/sqlite3.html)
public typealias SQLiteDatabaseConnection = OpaquePointer

/// The content pointer is constant and will never change.
///
/// - seealso: [Constants Defining Special Destructor Behavior](https://sqlite.org/c3ref/c_static.html)
public let SQLiteStaticStorage = unsafeBitCast(0, to: sqlite3_destructor_type.self)

/// The content will likely change in the near future and that SQLite should make its own private copy of the content before returning.
///
/// - seealso: [Constants Defining Special Destructor Behavior](https://sqlite.org/c3ref/c_static.html)
public let SQLiteTransientStorage = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

/// An [SQLite](https://sqlite.org) database.
public final class Database {
	/// The underlying `sqlite3 *` database.
	let databaseConnection: SQLiteDatabaseConnection

	/// The database's custom busy handler.
	var busyHandler: UnsafeMutablePointer<BusyHandler>?

	/// Creates a database from an existing `sqlite3 *` database connection handle.
	///
	/// - attention: The database takes ownership of `databaseConnection`.  The result of further use of `databaseConnection` is undefined.
	///
	/// - parameter databaseConnection: An `sqlite3 *` database connection handle.
	public init(databaseConnection: SQLiteDatabaseConnection) {
		self.databaseConnection = databaseConnection
	}

	deinit {
		_ = sqlite3_close(databaseConnection)
//		_ = sqlite3_close_v2(databaseConnection)
		busyHandler?.deinitialize(count: 1)
		busyHandler?.deallocate()
	}

	/// Creates a temporary database.
	///
	/// - parameter inMemory: Whether the temporary database should be created in-memory or on-disk.
	///
	/// - throws: An error if the database could not be created.
	public convenience init(inMemory: Bool = true) throws {
		var db: SQLiteDatabaseConnection?
		let path = inMemory ? ":memory:" : ""
		let result = sqlite3_open_v2(path, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil)
		guard result == SQLITE_OK else {
			_ = sqlite3_close(db)
			throw SQLiteError("Error creating temporary database", code: result)
		}
		precondition(db != nil)
		self.init(databaseConnection: db.unsafelyUnwrapped)
	}

	/// Creates a read-only database from a file.
	///
	/// - parameter url: The location of the SQLite database.
	///
	/// - throws: An error if the database could not be created.
	public convenience init(readingFrom url: URL) throws {
		var db: SQLiteDatabaseConnection?
		try url.withUnsafeFileSystemRepresentation { path in
			let result = sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY, nil)
			guard result == SQLITE_OK else {
				_ = sqlite3_close(db)
				throw SQLiteError("Error opening database \(url)", code: result)
			}
		}
		precondition(db != nil)
		self.init(databaseConnection: db.unsafelyUnwrapped)
	}

	/// Creates a database from a file.
	///
	/// - parameter url: The location of the SQLite database.
	/// - parameter create: Whether to create the database if it doesn't exist.
	///
	/// - throws: An error if the database could not be created.
	public convenience init(url: URL, create: Bool = true) throws {
		var db: SQLiteDatabaseConnection?
		try url.withUnsafeFileSystemRepresentation { path in
			var flags = SQLITE_OPEN_READWRITE
			if create {
				flags |= SQLITE_OPEN_CREATE
			}
			let result = sqlite3_open_v2(path, &db, flags, nil)
			guard result == SQLITE_OK else {
				_ = sqlite3_close(db)
				throw SQLiteError("Error opening database \(url)", code: result)
			}
		}
		precondition(db != nil)
		self.init(databaseConnection: db.unsafelyUnwrapped)
	}

#if canImport(Combine)

	/// The subject sending events from the update hook.
	lazy var tableChangeEventSubject: PassthroughSubject<TableChangeEvent, Never> = {
		let subject = PassthroughSubject<TableChangeEvent, Never>()
		setUpdateHook {
			subject.send($0)
		}
		return subject
	}()

#endif
}

extension Database {
	/// `true` if this database is read only, `false` otherwise.
	public var isReadOnly: Bool {
		sqlite3_db_readonly(self.databaseConnection, nil) == 1
	}

	/// The rowid of the most recent successful `INSERT` into a rowid table or virtual table.
	public var lastInsertRowid: Int64 {
		get {
			sqlite3_last_insert_rowid(databaseConnection)
		}
		set {
			sqlite3_set_last_insert_rowid(databaseConnection, newValue)
		}
	}

	/// The number of rows modified, inserted, or deleted by the most recently completed `INSERT`, `UPDATE` or `DELETE` statement.
	public var changes: Int64 {
		sqlite3_changes64(databaseConnection)
	}

	/// The total number of rows inserted, modified, or deleted by all `INSERT`, `UPDATE` or `DELETE` statements.
	public var totalChanges: Int64 {
		sqlite3_total_changes64(databaseConnection)
	}

	/// Interrupts a long-running query.
	public func interrupt() {
		sqlite3_interrupt(databaseConnection)
	}

	/// Returns the name of the *n*th attached database.
	///
	/// - note: 0 is the main database file and is named *main*.
	/// - note: 1 is the temporary schema and is named *temp*.
	///
	/// - parameter n: The index of the desired attached database.
	///
	/// - throws: An error if there is no attached database with the specified index.
	///
	/// - returns: The name of the *n*th attached database.
	public func name(ofDatabase n: Int32) throws -> String {
		guard let name = sqlite3_db_name(databaseConnection, n) else {
			throw DatabaseError(message: "The database at index \(n) does not exist")
		}
		return String(cString: name)
	}

	/// Returns the location of the file associated with database `name`.
	///
	/// - note: The main database file has the name *main*.
	///
	/// - parameter name: The name of the attached database whose location is desired.
	///
	/// - throws: An error if there is no attached database with the specified name, or if `name` is a temporary or in-memory database.
	///
	/// - returns: The URL for the file associated with database `name`.
	public func url(forDatabase name: String = "main") throws -> URL {
		guard let path = sqlite3_db_filename(databaseConnection, name) else {
			throw DatabaseError(message: "The database \"\(name)\" does not exist or is a temporary or in-memory database")
		}
		return URL(fileURLWithPath: String(cString: path))
	}

	/// Performs a low-level SQLite database operation.
	///
	/// - attention: **Use of this function should be avoided whenever possible.**
	///
	/// - parameter block: A closure performing the database operation.
	/// - parameter databaseConnection: The raw `sqlite3 *` database connection handle.
	///
	/// - throws: Any error thrown in `block`.
	///
	/// - returns: The value returned by `block`.
	public func withUnsafeSQLiteDatabaseConnection<T>(block: (_ databaseConnection: SQLiteDatabaseConnection) throws -> (T)) rethrows -> T {
		try block(databaseConnection)
	}
}

extension Database {
	/// Executes an SQL statement.
	///
	/// - parameter sql: The SQL statement to execute.
	///
	/// - throws: An error if `sql` could not be compiled or executed.
	public func execute(sql: String) throws {
		let result = sqlite3_exec(databaseConnection, sql, nil, nil, nil)
		guard result == SQLITE_OK else {
			throw SQLiteError("Error executing SQL \"\(sql)\"", fromDatabaseConnection: databaseConnection)
		}
	}

	/// Compiles and returns an SQL statement.
	///
	/// - parameter sql: The SQL statement to compile.
	///
	/// - throws: An error if `sql` could not be compiled.
	///
	/// - returns: A compiled SQL statement.
	public func prepare(sql: String) throws -> Statement {
		try Statement(database: self, sql: sql)
	}

	/// Executes one or more SQL statements and optionally applies `block` to each result row.
	///
	/// Multiple SQL statements are separated with a semicolon (`;`).
	///
	/// - parameter sql: The SQL statement or statements to execute.
	/// - parameter block: An optional closure applied to each result row.
	/// - parameter row: A dictionary of returned data keyed by column name.
	///
	/// - throws: An error if `sql` could not be compiled or executed.
	public func batch(sql: String, _ block: ((_ row: [String: String]) -> Void)? = nil) throws {
		var result: Int32
		var errmsg: UnsafeMutablePointer<Int8>?
		if let block = block {
			let context_ptr = UnsafeMutablePointer<((_ row: [String: String]) -> Void)?>.allocate(capacity: 1)
			context_ptr.initialize(to: block)
			defer {
				context_ptr.deinitialize(count: 1)
				context_ptr.deallocate()
			}

			result = sqlite3_exec(databaseConnection, sql, { (context, count, raw_values, raw_names) -> Int32 in
				let values = UnsafeMutableBufferPointer<UnsafeMutablePointer<Int8>?>(start: raw_values, count: Int(count))
				let names = UnsafeMutableBufferPointer<UnsafeMutablePointer<Int8>?>(start: raw_names, count: Int(count))

				var row = [String: String]()
				for i in 0 ..< Int(count) {
					let raw_value = values[i].unsafelyUnwrapped
					let value = String(bytesNoCopy: raw_value, length: strlen(raw_value), encoding: .utf8, freeWhenDone: false).unsafelyUnwrapped
					let raw_name = names[i].unsafelyUnwrapped
					let name = String(bytesNoCopy: raw_name, length: strlen(raw_name), encoding: .utf8, freeWhenDone: false).unsafelyUnwrapped
					row[name] = value
				}

				let context_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: (([String: String]) -> Void).self)
				context_ptr.pointee(row)

				return SQLITE_OK
			}, context_ptr, &errmsg)
		} else {
			result = sqlite3_exec(databaseConnection, sql, nil, nil, &errmsg)
		}
		guard result == SQLITE_OK else {
			let details = errmsg != nil ? String(bytesNoCopy: errmsg.unsafelyUnwrapped, length: strlen(errmsg.unsafelyUnwrapped), encoding: .utf8, freeWhenDone: true).unsafelyUnwrapped : nil
			throw DatabaseError(message: "Error executing SQL", details: details)
		}
	}	
}

extension Database {
	/// Returns the result or error code associated with the most recent `sqlite3_` API call.
	public var errorCode: Int32 {
		sqlite3_errcode(databaseConnection) & 0xff
	}

	/// Returns the result or extended error code associated with the most recent `sqlite3_` API call.
	public var extendedErrorCode: Int32 {
		sqlite3_extended_errcode(databaseConnection)
	}

	/// Returns the result or error message associated with the most recent `sqlite3_` API call.
	public var errorMessage: String {
		String(cString: sqlite3_errmsg(databaseConnection))
	}

	/// Returns the offset in the input SQL of the token referenced by the most recent error or `nil` if none.
	public var errorOffset: Int? {
		let offset = sqlite3_error_offset(databaseConnection)
		guard offset != -1 else {
			return nil
		}
		return Int(offset)
	}

	/// Returns the error code or error number that caused the most recent I/O error or failure to open a file or `nil` if none.
	public var systemErrno: Int32? {
		let errno = sqlite3_system_errno(databaseConnection)
		guard errno != 0 else {
			return nil
		}
		return errno
	}
}

extension Database {
	/// Returns `true` if the last `sqlite3_` API call succeeded.
	public var success: Bool {
		let result = sqlite3_errcode(databaseConnection) & 0xff
		return result == SQLITE_OK || result == SQLITE_ROW || result == SQLITE_DONE
	}
}
