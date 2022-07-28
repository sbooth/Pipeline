//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

//#if CSQLITE_CARRAY

extension SQLParameter {
	/// Binds a collection of `Int32` values using the sqlite3 carray extension..
	/// - seealso: [The Carray() Table-Valued Function](https://www.sqlite.org/carray.html)
	public static func carray<C: Collection>(_ values: C) -> SQLParameter where C.Element == Int32 {
		SQLParameter { statement, index in
			let mem = UnsafeMutableBufferPointer<Int32>.allocate(capacity: values.count)
			_ = mem.initialize(from: values)
			guard sqlite3_carray_bind(statement.preparedStatement, Int32(index), mem.baseAddress, Int32(values.count), CARRAY_INT32, { $0?.deallocate() }) == SQLITE_OK else {
				throw SQLiteError("Error binding carray (CARRAY_INT32) to parameter \(index)", takingErrorCodeFromPreparedStatement: statement.preparedStatement)
			}
		}
	}

	/// Binds a collection of `Int64` values using the sqlite3 carray extension..
	/// - seealso: [The Carray() Table-Valued Function](https://www.sqlite.org/carray.html)
	public static func carray<C: Collection>(_ values: C) -> SQLParameter where C.Element == Int64 {
		SQLParameter { statement, index in
			let mem = UnsafeMutableBufferPointer<Int64>.allocate(capacity: values.count)
			_ = mem.initialize(from: values)
			guard sqlite3_carray_bind(statement.preparedStatement, Int32(index), mem.baseAddress, Int32(values.count), CARRAY_INT64, { $0?.deallocate() }) == SQLITE_OK else {
				throw SQLiteError("Error binding carray (CARRAY_INT64) to parameter \(index)", takingErrorCodeFromPreparedStatement: statement.preparedStatement)
			}
		}
	}

	/// Binds a collection of doubles using the sqlite3 carray extension..
	/// - seealso: [The Carray() Table-Valued Function](https://www.sqlite.org/carray.html)
	public static func carray<C: Collection>(_ values: C) -> SQLParameter where C.Element == Double {
		SQLParameter { statement, index in
			let mem = UnsafeMutableBufferPointer<Double>.allocate(capacity: values.count)
			_ = mem.initialize(from: values)
			guard sqlite3_carray_bind(statement.preparedStatement, Int32(index), mem.baseAddress, Int32(values.count), CARRAY_DOUBLE, { $0?.deallocate() }) == SQLITE_OK else {
				throw SQLiteError("Error binding carray (CARRAY_DOUBLE) to parameter \(index)", takingErrorCodeFromPreparedStatement: statement.preparedStatement)
			}
		}
	}

	/// Binds a collection of strings using the sqlite3 carray extension..
	/// - seealso: [The Carray() Table-Valued Function](https://www.sqlite.org/carray.html)
	public static func carray<C: Collection>(_ values: C) -> SQLParameter where C.Element == String {
		SQLParameter { statement, index in
			let count = values.count

			let utf8_character_counts = values.map { $0.utf8.count + 1 }
			let utf8_offsets = [ 0 ] + scan(utf8_character_counts, 0, +)
			let utf8_buf_size = utf8_offsets.last!

			let ptr_size = MemoryLayout<UnsafePointer<Int8>>.stride * count
			let alloc_size = ptr_size + utf8_buf_size

			let mem = UnsafeMutableRawPointer.allocate(byteCount: alloc_size, alignment: MemoryLayout<UnsafePointer<Int8>>.alignment)

			let ptrs = mem.bindMemory(to: UnsafeMutablePointer<Int8>.self, capacity: count)
			let utf8 = (mem + ptr_size).bindMemory(to: Int8.self, capacity: utf8_buf_size)

			for(i, s) in values.enumerated() {
				let pos = utf8 + utf8_offsets[i]
				ptrs[i] = pos
				memcpy(pos, s, utf8_offsets[i + 1] - utf8_offsets[i])
			}

			guard sqlite3_carray_bind(statement.preparedStatement, Int32(index), mem, Int32(values.count), CARRAY_TEXT, { $0?.deallocate() }) == SQLITE_OK else {
				throw SQLiteError("Error binding carray (CARRAY_TEXT) to parameter \(index)", takingErrorCodeFromPreparedStatement: statement.preparedStatement)
			}
		}
	}
}

/// Computes and returns the prefix sum of a sequence.
/// - parameter sequence: A sequence.
/// - parameter initialResult: The value to use as the initial accumulating value. `initialResult` is passed to `nextPartialResult` the first time the closure is executed.
/// - parameter nextPartialResult: A closure that combines an accumulating value and an element of the sequence into a new accumulating value, to be used in the next call of the `nextPartialResult` closure or returned to the caller.
/// - returns: The prefix sum. If the sequence has no elements, the result is `initialResult`.
private func scan<S, Result>(_ sequence: S, _ initialResult: Result, _ nextPartialResult: (Result, S.Element) -> Result) -> [Result] where S: Sequence {
	var result: [Result] = []
	result.reserveCapacity(sequence.underestimatedCount)
	var runningResult = initialResult
	for element in sequence {
		runningResult = nextPartialResult(runningResult, element)
		result.append(runningResult)
	}
	return result
}

//#endif
