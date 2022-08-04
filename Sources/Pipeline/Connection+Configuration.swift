//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

extension Connection {
	/// Enables or disables the enforcement of foreign key constraints for the database connection.
	/// - seealso: [SQLite Foreign Key Support](https://sqlite.org/foreignkeys.html)
	public var enforcesForeignKeyConstraints: Bool {
		get {
			var enabled: Int32 = 0
			_ = csqlite_sqlite3_db_config_enable_fkey(databaseConnection, -1, &enabled)
			return enabled != 0
		}
		set {
			_ = csqlite_sqlite3_db_config_enable_fkey(databaseConnection, newValue ? 1 : 0, nil)
		}
	}

	/// Enables or disables triggers for the database connection.
	/// - seealso: [CREATE TRIGGER](https://sqlite.org/lang_createtrigger.html)
	public var triggersAreEnabled: Bool {
		get {
			var enabled: Int32 = 0
			_ = csqlite_sqlite3_db_config_enable_trigger(databaseConnection, -1, &enabled)
			return enabled != 0
		}
		set {
			_ = csqlite_sqlite3_db_config_enable_trigger(databaseConnection, newValue ? 1 : 0, nil)
		}
	}

	/// Enables or disables views for the database connection.
	/// - seealso: [CREATE VIEW](https://sqlite.org/lang_createview.html)
	public var viewsAreEnabled: Bool {
		get {
			var enabled: Int32 = 0
			_ = csqlite_sqlite3_db_config_enable_view(databaseConnection, -1, &enabled)
			return enabled != 0
		}
		set {
			_ = csqlite_sqlite3_db_config_enable_view(databaseConnection, newValue ? 1 : 0, nil)
		}
	}

	/// Enables or disables `sqlite3_load_extension()` independent of the `load_extension()` SQL function.
	/// - seealso: [Enable Or Disable Extension Loading](https://sqlite.org/c3ref/enable_load_extension.html)
	/// - seealso: [Load An Extension](https://sqlite.org/c3ref/load_extension.html)
	/// - seealso: [load_extension(X)](https://sqlite.org/lang_corefunc.html#load_extension)
	public var extensionLoadingIsEnabled: Bool {
		get {
			var enabled: Int32 = 0
			_ = csqlite_sqlite3_db_config_enable_load_extension(databaseConnection, -1, &enabled)
			return enabled != 0
		}
		set {
			_ = csqlite_sqlite3_db_config_enable_load_extension(databaseConnection, newValue ? 1 : 0, nil)
		}
	}

	/// Enables or disables a checkpoint operation for the database connection when closing the last connection to a database in WAL mode.
	public var checkpointOnCloseIsDisabled: Bool {
		get {
			var enabled: Int32 = 0
			_ = csqlite_sqlite3_db_config_no_ckpt_on_close(databaseConnection, -1, &enabled)
			return enabled != 0
		}
		set {
			_ = csqlite_sqlite3_db_config_no_ckpt_on_close(databaseConnection, newValue ? 1 : 0, nil)
		}
	}

	/// Enables or disables the query planner stability guarantee for the database connection.
	/// - seealso: [The SQLite Query Planner Stability Guarantee](https://sqlite.org/queryplanner-ng.html#qpstab)
	public var queryPlannerStabilityGuarantee: Bool {
		get {
			var enabled: Int32 = 0
			_ = csqlite_sqlite3_db_config_enable_qpsg(databaseConnection, -1, &enabled)
			return enabled != 0
		}
		set {
			_ = csqlite_sqlite3_db_config_enable_qpsg(databaseConnection, newValue ? 1 : 0, nil)
		}
	}

	/// Enables or disables the defensive flag for the database connection.
	///
	/// When enabled the defensive flag disables language features that allow ordinary SQL to deliberately corrupt the database.
	public var defensive: Bool {
		get {
			var enabled: Int32 = 0
			_ = csqlite_sqlite3_db_config_defensive(databaseConnection, -1, &enabled)
			return enabled != 0
		}
		set {
			_ = csqlite_sqlite3_db_config_defensive(databaseConnection, newValue ? 1 : 0, nil)
		}
	}

	/// Enables or disables the `writable_schema` flag  for the database connection.
	/// - seealso: [PRAGMA writable_schema] (https://sqlite.org/pragma.html#pragma_writable_schema)
	public var writableSchema: Bool {
		get {
			var enabled: Int32 = 0
			_ = csqlite_sqlite3_db_config_writable_schema(databaseConnection, -1, &enabled)
			return enabled != 0
		}
		set {
			_ = csqlite_sqlite3_db_config_writable_schema(databaseConnection, newValue ? 1 : 0, nil)
		}
	}

	/// Enables or disables the legacy `ALTER TABLE RENAME` behavior  for the database connection.
	/// - seealso: [PRAGMA legacy_alter_table](https://sqlite.org/pragma.html#pragma_legacy_alter_table)
	/// - seealso: [ALTER TABLE RENAME](https://sqlite.org/lang_altertable.html#altertabrename)
	public var legacyAlterTableBehavior: Bool {
		get {
			var enabled: Int32 = 0
			_ = csqlite_sqlite3_db_config_legacy_alter_table(databaseConnection, -1, &enabled)
			return enabled != 0
		}
		set {
			_ = csqlite_sqlite3_db_config_legacy_alter_table(databaseConnection, newValue ? 1 : 0, nil)
		}
	}

	/// Activates or deactivates the legacy double-quoted string "misfeature" for the database connection for DML statements only.
	///
	/// DML statements include `DELETE`, `INSERT`, `SELECT`, and `UPDATE` statements.
	/// - seealso: [Double-quoted String Literals Are Accepted](https://sqlite.org/quirks.html#dblquote)
	public var doubleQuotedStringsInDMLAreEnabled: Bool {
		get {
			var enabled: Int32 = 0
			_ = csqlite_sqlite3_db_config_dqs_dml(databaseConnection, -1, &enabled)
			return enabled != 0
		}
		set {
			_ = csqlite_sqlite3_db_config_dqs_dml(databaseConnection, newValue ? 1 : 0, nil)
		}
	}

	/// Activates or deactivates the legacy double-quoted string "misfeature" for the database connection for DDL statements.
	///
	/// DDL statements include `CREATE TABLE` and `CREATE INDEX`.
	/// - seealso: [Double-quoted String Literals Are Accepted](https://sqlite.org/quirks.html#dblquote)
	public var doubleQuotedStringsInDDLAreEnabled: Bool {
		get {
			var enabled: Int32 = 0
			_ = csqlite_sqlite3_db_config_dqs_ddl(databaseConnection, -1, &enabled)
			return enabled != 0
		}
		set {
			_ = csqlite_sqlite3_db_config_dqs_ddl(databaseConnection, newValue ? 1 : 0, nil)
		}
	}

	/// Tells SQLite to assume that database schemas for the database connection are untainted by maliicious content.
	/// - seealso: [PRAGMA trusted_schema](https://sqlite.org/pragma.html#pragma_trusted_schema)
	public var trustedSchema: Bool {
		get {
			var enabled: Int32 = 0
			_ = csqlite_sqlite3_db_config_trusted_schema(databaseConnection, -1, &enabled)
			return enabled != 0
		}
		set {
			_ = csqlite_sqlite3_db_config_trusted_schema(databaseConnection, newValue ? 1 : 0, nil)
		}
	}
}
