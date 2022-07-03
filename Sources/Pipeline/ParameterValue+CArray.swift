//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

//#if CSQLITE_CARRAY

extension ParameterValue {
	/// Binds a `Collection` of `Int32` values using the sqlite3 carray extension..
	/// - seealso: [The Carray() Table-Valued Function](https://www.sqlite.org/carray.html)
	public static func carray<C: Collection>(values: C) -> ParameterValue where C.Element == Int32 {
		ParameterValue { statement, index in
			let mem = UnsafeMutableBufferPointer<Int32>.allocate(capacity: values.count)
			_ = mem.initialize(from: values)
			guard sqlite3_carray_bind(statement.preparedStatement, Int32(index), mem.baseAddress, Int32(values.count), CARRAY_INT32, { $0?.deallocate() }) == SQLITE_OK else {
				throw SQLiteError(fromPreparedStatement: statement.preparedStatement)
			}
		}
	}

	/// Binds a `Collection` of `Int64` values using the sqlite3 carray extension..
	/// - seealso: [The Carray() Table-Valued Function](https://www.sqlite.org/carray.html)
	public static func carray<C: Collection>(values: C) -> ParameterValue where C.Element == Int64 {
		ParameterValue { statement, index in
			let mem = UnsafeMutableBufferPointer<Int64>.allocate(capacity: values.count)
			_ = mem.initialize(from: values)
			guard sqlite3_carray_bind(statement.preparedStatement, Int32(index), mem.baseAddress, Int32(values.count), CARRAY_INT64, { $0?.deallocate() }) == SQLITE_OK else {
				throw SQLiteError(fromPreparedStatement: statement.preparedStatement)
			}
		}
	}

	/// Binds a `Collection` of `Double` values using the sqlite3 carray extension..
	/// - seealso: [The Carray() Table-Valued Function](https://www.sqlite.org/carray.html)
	public static func carray<C: Collection>(values: C) -> ParameterValue where C.Element == Double {
		ParameterValue { statement, index in
			let mem = UnsafeMutableBufferPointer<Double>.allocate(capacity: values.count)
			_ = mem.initialize(from: values)
			guard sqlite3_carray_bind(statement.preparedStatement, Int32(index), mem.baseAddress, Int32(values.count), CARRAY_DOUBLE, { $0?.deallocate() }) == SQLITE_OK else {
				throw SQLiteError(fromPreparedStatement: statement.preparedStatement)
			}
		}
	}

	/// Binds a `Collection` of `String` values using the sqlite3 carray extension..
	/// - seealso: [The Carray() Table-Valued Function](https://www.sqlite.org/carray.html)
	public static func carray<C: Collection>(values: C) -> ParameterValue where C.Element == String {
		ParameterValue { statement, index in
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
				throw SQLiteError(fromPreparedStatement: statement.preparedStatement)
			}
		}
	}
}

/// Computes the accumulated result  of `seq`.
private func accumulate<S: Sequence, U>(_ seq: S, _ initial: U, _ combine: (U, S.Element) -> U) -> [U] {
	var result: [U] = []
	result.reserveCapacity(seq.underestimatedCount)
	var runningResult = initial
	for element in seq {
		runningResult = combine(runningResult, element)
		result.append(runningResult)
	}
	return result
}

/// Computes the prefix sum of `seq`.
private func scan<S: Sequence, U>(_ seq: S, _ initial: U, _ combine: (U, S.Element) -> U) -> [U] {
	var result: [U] = []
	result.reserveCapacity(seq.underestimatedCount)
	var runningResult = initial
	for element in seq {
		runningResult = combine(runningResult, element)
		result.append(runningResult)
	}
	return result
}

//#endif
