//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

extension Database {
	/// A hook called when a database transaction is committed.
	///
	/// - returns: `true` if the commit operation is allowed to proceed, `false` otherwise.
	///
	/// - seealso: [Commit And Rollback Notification Callbacks](https://www.sqlite.org/c3ref/commit_hook.html)
	public typealias CommitHook = () -> Bool

	/// Sets the hook called when a database transaction is committed.
	///
	/// - parameter commitHook: A closure called when a transaction is committed.
	public func setCommitHook(_ block: @escaping CommitHook) {
		let context = UnsafeMutablePointer<CommitHook>.allocate(capacity: 1)
		context.initialize(to: block)
		if let old = sqlite3_commit_hook(databaseConnection, {
			$0.unsafelyUnwrapped.assumingMemoryBound(to: CommitHook.self).pointee() ? 0 : 1
		}, context) {
			let oldContext = old.assumingMemoryBound(to: CommitHook.self)
			oldContext.deinitialize(count: 1)
			oldContext.deallocate()
		}
	}

	/// Removes the commit hook.
	public func removeCommitHook() {
		if let old = sqlite3_commit_hook(databaseConnection, nil, nil) {
			let oldContext = old.assumingMemoryBound(to: CommitHook.self)
			oldContext.deinitialize(count: 1)
			oldContext.deallocate()
		}
	}

	/// A hook called when a database transaction is rolled back.
	///
	/// - seealso: [Commit And Rollback Notification Callbacks](https://www.sqlite.org/c3ref/commit_hook.html)
	public typealias RollbackHook = () -> Void

	/// Sets the hook called when a database transaction is rolled back.
	///
	/// - parameter rollbackHook: A closure called when a transaction is rolled back.
	public func setRollbackHook(_ block: @escaping RollbackHook) {
		let context = UnsafeMutablePointer<RollbackHook>.allocate(capacity: 1)
		context.initialize(to: block)
		if let old = sqlite3_rollback_hook(databaseConnection, {
			$0.unsafelyUnwrapped.assumingMemoryBound(to: RollbackHook.self).pointee()
		}, context) {
			let oldContext = old.assumingMemoryBound(to: RollbackHook.self)
			oldContext.deinitialize(count: 1)
			oldContext.deallocate()
		}
	}

	/// Removes the rollback hook.
	public func removeRollbackHook() {
		if let old = sqlite3_rollback_hook(databaseConnection, nil, nil) {
			let oldContext = old.assumingMemoryBound(to: RollbackHook.self)
			oldContext.deinitialize(count: 1)
			oldContext.deallocate()
		}
	}
}

extension Database {
	/// A hook called when a database transaction is committed in write-ahead log mode.
	///
	/// - parameter databaseName: The name of the database that was written to.
	/// - parameter pageCount: The number of pages in the write-ahead log file.
	///
	/// - returns: Normally `SQLITE_OK`.
	///
	/// - seealso: [Write-Ahead Log Commit Hook](https://www.sqlite.org/c3ref/wal_hook.html)
	public typealias WALCommitHook = (_ databaseName: String, _ pageCount: Int) -> Int32

	/// Sets the hook called when a database transaction is committed in write-ahead log mode.
	///
	/// - parameter commitHook: A closure called when a transaction is committed.
	public func setWALCommitHook(_ block: @escaping WALCommitHook) {
		let context = UnsafeMutablePointer<WALCommitHook>.allocate(capacity: 1)
		context.initialize(to: block)
		if let old = sqlite3_wal_hook(databaseConnection, { context, database_connection, database_name, pageCount in
//			guard database_connection == self.databaseConnection else {
//				fatalError("Unexpected database connection handle from sqlite3_wal_hook")
//			}
			let database = String(utf8String: database_name.unsafelyUnwrapped).unsafelyUnwrapped
			return context.unsafelyUnwrapped.assumingMemoryBound(to: WALCommitHook.self).pointee(database, Int(pageCount))
		}, context) {
			let oldContext = old.assumingMemoryBound(to: WALCommitHook.self)
			oldContext.deinitialize(count: 1)
			oldContext.deallocate()
		}
	}

	/// Removes the write-ahead log commit hook.
	public func removeWALCommitHook() {
		if let old = sqlite3_wal_hook(databaseConnection, nil, nil) {
			let oldContext = old.assumingMemoryBound(to: WALCommitHook.self)
			oldContext.deinitialize(count: 1)
			oldContext.deallocate()
		}
	}
}

extension Database {
	/// Possible types of row changes.
	public enum RowChangeType {
		/// A row was inserted.
		case insert
		/// A row was deleted.
		case delete
		/// A row was updated.
		case update
	}

	/// An insert, delete, or update event on a rowid table.
	public struct TableChangeEvent {
		/// The type of row change.
		public let changeType: RowChangeType
		/// The name of the database containing the table that changed.
		public let database: String
		/// The name of the table that changed.
		public let table: String
		/// The rowid of the row that changed.
		public let rowid: Int64
	}

	/// A hook called when a row is inserted, deleted, or updated in a rowid table.
	///
	/// - parameter changeEvent: The table change event triggering the hook.
	///
	/// - seealso: [Commit And Rollback Notification Callbacks](https://www.sqlite.org/c3ref/commit_hook.html)
	/// - seealso: [Rowid Tables](https://www.sqlite.org/rowidtable.html)
	public typealias UpdateHook = (_ changeEvent: TableChangeEvent) -> Void

	/// Sets the hook called when a row is inserted, deleted, or updated in a rowid table.
	///
	/// - parameter updateHook: A closure called when a row is inserted, deleted, or updated.
	public func setUpdateHook(_ block: @escaping UpdateHook) {
		let context = UnsafeMutablePointer<UpdateHook>.allocate(capacity: 1)
		context.initialize(to: block)
		if let old = sqlite3_update_hook(databaseConnection, { context, op, database_name, table_name, rowid in
			let function_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: UpdateHook.self)
			let changeType = RowChangeType(op)
			let database = String(utf8String: database_name.unsafelyUnwrapped).unsafelyUnwrapped
			let table = String(utf8String: table_name.unsafelyUnwrapped).unsafelyUnwrapped
			let changeEvent = TableChangeEvent(changeType: changeType, database: database, table: table, rowid: rowid)
			function_ptr.pointee(changeEvent)
		}, context) {
			let oldContext = old.assumingMemoryBound(to: UpdateHook.self)
			oldContext.deinitialize(count: 1)
			oldContext.deallocate()
		}
	}

	/// Removes the update hook.
	public func removeUpdateHook() {
		if let old = sqlite3_update_hook(databaseConnection, nil, nil) {
			let oldContext = old.assumingMemoryBound(to: UpdateHook.self)
			oldContext.deinitialize(count: 1)
			oldContext.deallocate()
		}
	}
}

extension Database.RowChangeType {
	/// Convenience initializer for conversion of `SQLITE_` values.
	///
	/// - parameter operation: The second argument to the callback function passed to `sqlite3_update_hook()`.
	init(_ operation: Int32) {
		switch operation {
		case SQLITE_INSERT:
			self = .insert
		case SQLITE_DELETE:
			self = .delete
		case SQLITE_UPDATE:
			self = .update
		default:
			fatalError("Unexpected SQLite row change type \(operation)")
		}
	}
}

#if canImport(Combine)

import Combine

extension Database {
	/// Returns a publisher for changes to rowid tables.
	///
	/// - note: The publisher uses the database's update hook for event generation. If this
	/// publisher is used the update hook is unavailable.
	///
	/// - returns: A publisher for changes to the database's rowid tables.
	public var tableChangeEventPublisher: AnyPublisher<TableChangeEvent, Never> {
		tableChangeEventSubject
			.eraseToAnyPublisher()
	}
}

#endif

extension Database {
	/// A hook that may be called when an attempt is made to access a locked database table.
	///
	/// - parameter attempts: The number of times the busy handler has been called for the same event.
	///
	/// - returns: `true` if the attempts to access the database should stop, `false` to continue.
	///
	/// - seealso: [Register A Callback To Handle SQLITE_BUSY Errors](https://www.sqlite.org/c3ref/busy_handler.html)
	public typealias BusyHandler = (_ attempts: Int) -> Bool

	/// Sets a callback that may be invoked when an attempt is made to access a locked database table.
	///
	/// - parameter busyHandler: A closure called when an attempt is made to access a locked database table.
	///
	/// - throws: An error if the busy handler couldn't be set.
	public func setBusyHandler(_ block: @escaping BusyHandler) throws {
		if busyHandler == nil {
			busyHandler = UnsafeMutablePointer<BusyHandler>.allocate(capacity: 1)
		} else {
			busyHandler?.deinitialize(count: 1)
		}
		busyHandler?.initialize(to: block)
		guard sqlite3_busy_handler(databaseConnection, { context, count in
			return context.unsafelyUnwrapped.assumingMemoryBound(to: BusyHandler.self).pointee(Int(count)) ? 0 : 1
		}, busyHandler) == SQLITE_OK else {
			busyHandler?.deinitialize(count: 1)
			busyHandler?.deallocate()
			busyHandler = nil
			throw DatabaseError(message: "Error setting busy handler")
		}
	}

	/// Removes the busy handler.
	///
	/// - throws: An error if the busy handler couldn't be removed.
	public func removeBusyHandler() throws {
		defer {
			busyHandler?.deinitialize(count: 1)
			busyHandler?.deallocate()
			busyHandler = nil
		}
		guard sqlite3_busy_handler(databaseConnection, nil, nil) == SQLITE_OK else {
			throw SQLiteError(fromDatabaseConnection: databaseConnection)
		}
	}

	/// Sets a busy handler that sleeps when an attempt is made to access a locked database table.
	///
	/// - parameter ms: The minimum time in milliseconds to sleep.
	///
	/// - throws: An error if the busy timeout couldn't be set.
	///
	/// - seealso: [Set A Busy Timeout](https://www.sqlite.org/c3ref/busy_timeout.html)
	public func setBusyTimeout(_ ms: Int) throws {
		defer {
			busyHandler?.deinitialize(count: 1)
			busyHandler?.deallocate()
			busyHandler = nil
		}
		guard sqlite3_busy_timeout(databaseConnection, Int32(ms)) == SQLITE_OK else {
			throw DatabaseError(message: "Error setting busy timeout")
		}
	}
}

// The pre-update hook is not compiled into Pipeline by default
// because it is not one of the recommended SQLite compile-time
// options: https://www.sqlite.org/compile.html
// To enable it uncomment the appropriate lines in Package.swift
#if SQLITE_ENABLE_PREUPDATE_HOOK

extension Database {
	/// Information on an insert, update, or delete operation in a pre-update context.
	///
	/// - seealso: [The pre-update hook.](https://sqlite.org/c3ref/preupdate_count.html)
	public struct PreUpdateChange {
		/// Possible types of pre-update changes with associated rowids.
		///
		/// - seealso: [The pre-update hook.](https://sqlite.org/c3ref/preupdate_count.html)
		public enum	ChangeType {
			/// A row was inserted.
			case insert(Int64)
			/// A row was deleted.
			case delete(Int64)
			/// A row was updated.
			case update(Int64, Int64)
		}

		/// The underlying `sqlite3 *` database.
		let databaseConnection: SQLiteDatabaseConnection

		/// The type of pre-update change.
		public let changeType: ChangeType
		/// The name of the database being changed.
		public let database: String
		/// The name of the table being changed.
		public let table: String

		/// Returns the number of columns in the row that is being inserted, updated, or deleted.
		public var count: Int {
			Int(sqlite3_preupdate_count(databaseConnection))
		}

		/// Returns 0 if the pre-update callback was invoked as a result of a direct insert, update, or delete operation; or 1 for inserts, updates, or deletes invoked by top-level triggers; or 2 for changes resulting from triggers called by top-level triggers; and so forth.
		public var depth: Int {
			Int(sqlite3_preupdate_depth(databaseConnection))
		}

		/// Returns the value for the column at `index` of the table row before it is updated.
		///
		/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
		///
		/// - requires: `index >= 0`.
		/// - requires: `index < self.count`.
		///
		/// - note: This is only valid for `.update` and `.delete` change types.
		///
		/// - parameter index: The index of the desired column.
		///
		/// - throws: An error if `index` is out of bounds or an other error occurs.
		public func oldValue(at index: Int) throws -> DatabaseValue {
			if case .insert(_) = changeType {
				throw DatabaseError(message: "sqlite3_preupdate_old() is undefined for insertions")
			}
			var value: SQLiteValue?
			guard sqlite3_preupdate_old(databaseConnection, Int32(index), &value) == SQLITE_OK else {
				throw SQLiteError(fromDatabaseConnection: databaseConnection)
			}
			return DatabaseValue(sqliteValue: value.unsafelyUnwrapped)
		}

		/// Returns the value for the column at `index` of the table row after it is updated.
		///
		/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
		///
		/// - requires: `index >= 0`.
		/// - requires: `index < self.count`.
		///
		/// - note: This is only valid for `.update` and `.insert` row change types.
		///
		/// - parameter index: The index of the desired column.
		///
		/// - throws: An error if `index` is out of bounds or an other error occurs.
		public func newValue(at index: Int) throws -> DatabaseValue {
			if case .delete(_) = changeType {
				throw DatabaseError(message: "sqlite3_preupdate_new() is undefined for deletions")
			}
			var value: SQLiteValue?
			guard sqlite3_preupdate_new(databaseConnection, Int32(index), &value) == SQLITE_OK else {
				throw SQLiteError(fromDatabaseConnection: databaseConnection)
			}
			return DatabaseValue(sqliteValue: value.unsafelyUnwrapped)
		}
	}

	/// A hook called before a row is inserted, deleted, or updated.
	///
	/// - parameter change: The change triggering the pre-update hook.
	///
	/// - seealso: [The pre-update hook.](https://sqlite.org/c3ref/preupdate_count.html)
	public typealias PreUpdateHook = (_ context: PreUpdateChange) -> Void

	/// Sets the hook called before a row is inserted, deleted, or updated.
	///
	/// - parameter block: A closure called before a row is inserted, deleted, or updated.
	///
	/// - seealso: [The pre-update hook.](https://sqlite.org/c3ref/preupdate_count.html)
	public func setPreUpdateHook(_ block: @escaping PreUpdateHook) {
		let context = UnsafeMutablePointer<PreUpdateHook>.allocate(capacity: 1)
		context.initialize(to: block)

		if let old = sqlite3_preupdate_hook(databaseConnection, { context, db, op, db_name, table_name, existing_rowid, new_rowid in
			let function_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: PreUpdateHook.self)

			let changeType = Database.PreUpdateChange.ChangeType(op: op, key1: existing_rowid, key2: new_rowid)
			let database = String(utf8String: db_name.unsafelyUnwrapped).unsafelyUnwrapped
			let table = String(utf8String: table_name.unsafelyUnwrapped).unsafelyUnwrapped

			let update = PreUpdateChange(databaseConnection: db.unsafelyUnwrapped, changeType: changeType, database: database, table: table)
			function_ptr.pointee(update)
		}, context) {
			let oldContext = old.assumingMemoryBound(to: PreUpdateHook.self)
			oldContext.deinitialize(count: 1)
			oldContext.deallocate()
		}
	}

	/// Removes the pre-update hook.
	public func removePreUpdateHook() {
		if let old = sqlite3_preupdate_hook(databaseConnection, nil, nil) {
			let oldContext = old.assumingMemoryBound(to: PreUpdateHook.self)
			oldContext.deinitialize(count: 1)
			oldContext.deallocate()
		}
	}
}

extension Database.PreUpdateChange.ChangeType {
	/// Convenience initializer for conversion of `SQLITE_` values and associated rowids.
	///
	/// - parameter op: The third argument to the callback function passed to `sqlite3_preupdate_hook()`.
	/// - parameter key1: The sixth argument to the callback function passed to `sqlite3_preupdate_hook()`.
	/// - parameter key2: The seventh argument to the callback function passed to `sqlite3_preupdate_hook()`.
	init(op: Int32, key1: Int64, key2: Int64) {
		switch op {
		case SQLITE_INSERT:
			self = .insert(key2)
		case SQLITE_DELETE:
			self = .delete(key1)
		case SQLITE_UPDATE:
			self = .update(key1, key2)
		default:
			fatalError("Unexpected pre-update hook row change type \(op)")
		}
	}
}

#endif
