/*
 ** 2021-05-29
 **
 ** The author disclaims copyright to this source code.  In place of
 ** a legal notice, here is a blessing:
 **
 **    May you do good and not evil.
 **    May you find forgiveness for yourself and forgive others.
 **    May you share freely, never taking more than you give.
 **
 ******************************************************************************
 **
 ** C structure definitions for Pipeline virtual tables.
 */

#include "sqlite3.h"

/// A struct to allow creation of SQLite virtual tables in Swift.
struct cpipeline_sqlite3_vtab {
	/// sqlite3 required fields
	sqlite3_vtab base;
	/// @c UnsafeMutablePointer<VirtualTableModule>
	void *virtual_table_module_ptr;
};
typedef struct cpipeline_sqlite3_vtab cpipeline_sqlite3_vtab;

/// A struct to allow creation of SQLite virtual table cursors in Swift.
struct cpipeline_sqlite3_vtab_cursor {
	/// sqlite3 required fields
	sqlite3_vtab_cursor base;
	/// @c UnsafeMutablePointer<VirtualTableCursor>
	void *virtual_table_cursor_ptr;
};
typedef struct cpipeline_sqlite3_vtab_cursor cpipeline_sqlite3_vtab_cursor;
