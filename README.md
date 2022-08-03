# Pipeline

Pipeline is a powerful and performant Swift interface to [SQLite](https://sqlite.org) featuring:

- Type-safe and type-agnostic database values.
- Thread-safe synchronous and asynchronous database access.
- Full support for [transactions](#perform-a-transaction) and savepoints.
- [Custom SQL functions](#custom-sql-functions), including aggregate and window functions.
- [Custom collating sequences](#custom-collating-sequences).
- Custom commit, rollback, update, and busy handler hooks.
- Custom virtual tables.
- Custom FTS5 tokenizers.
- Optional support for pre-update hooks and [sessions](https://www.sqlite.org/sessionintro.html).
- [Combine](#combine) support.

Pipeline allows fast, easy database access with robust error handling.  It is not a general-purpose object-relational mapper.

## Installation

### Swift Package Manager

Add a package dependency to https://github.com/sbooth/Pipeline in Xcode.

### Manual or Custom Build

1. Clone the [Pipeline](https://github.com/sbooth/Pipeline) repository.
2. `swift build`.

## Quick Start

```swift
// Create a connection to an in-memory database
let connection = try Connection()

// Create a table
try connection.execute(sql: "CREATE TABLE t1(a,b);")

// Insert a row
try connection.execute(sql: "INSERT INTO t1(a,b) VALUES (?,?);", 
                       parameters: 33, "lulu")

// Retrieve the values
try connection.execute(sql: "SELECT a,b FROM t1;") { row in
    let a = try row.get(.int, at: 0)
    let b = try row.get(.string, at: 1)
}
```

### Segue to Thread Safety

Pipeline uses SQLite with thread safety disabled for improved performance. While this increases performance, it also means a `Connection` object may only be accessed from a single thread or dispatch queue at a time.

Most applications should not create a `Connection` directly but instead should use a thread-safe `ConnectionQueue`.

```swift
// Create a queue serializing access to an in-memory database
let connectionQueue = try ConnectionQueue("myapp.database-connection-isolation-queue")
```

This creates a queue which may be used from multiple threads or dispatch queues safely.  The queue serializes access to the database connection ensuring only a single operation occurs at a time. Database operations may be performed synchronously or asynchronously.

```swift
// Perform a synchronous database access
try connectionQueue.sync { connection in
    // Do something with `connection`
}

// Perform an asynchronous database access
connectionQueue.async { connection in
    // Do something with `connection`
} completion: { result in
    switch result {
        case .success:
            // 🎉
        case .failure(let error):
            // Handle any errors that occurred
    }
}
```

For databases using [Write-Ahead Logging](https://www.sqlite.org/wal.html) concurrent reading and writing is supported. Multiple read operations may be performed simultaneously using more than one `ConnectionReadQueue` instance.  Write operations must always be confined to a single `ConnectionQueue`.  A typical usage pattern is one global `ConnectionQueue` instance used for writing located in the application's delegate, with `ConnectionReadQueue` instances located in individual view or window controllers.  When used with long-running read transactions each `ConnectionReadQueue` maintains a separate, consistent snapshot of the database that may be updated in response to database changes.

## Design

The core of Pipeline is the types `Connection`, `Statement`, and `Row`.

- `Connection` is a connection to an SQLite database.

- `Statement` is a compiled SQL statement.

- `Row` is a single result row.

The fundamental type for a column value in a result row is `DatabaseValue`.

- `DatabaseValue` objects contain an integer, floating-point, textual, null, or BLOB value.

- `DatabaseValue` objects may be retrieved from result rows.

Type-safe column value access is provided by specializations of the `ColumnValueConverter` struct.

- `ColumnValueConverter<T>` converts a column value in a result row to an object of type `T`.

Type-safe SQL parameter binding is provided by `SQLParameter` objects.

- `SQLParameter` objects capture a value and bind it to an SQL parameter.

Thread-safe access to a database is provided by `ConnectionQueue`.

- `ConnectionQueue` serializes work items on a database connection.
- `ConnectionReadQueue` serializes read operations on a database connection.

## Examples

### Create a Connection to an In-Memory Database

```swift
let connection = try Connection()
```

This creates a connection for use on a single thread or dispatch queue only. Most applications should not create a `Connection` directly but instead should use a thread-safe `ConnectionQueue`.

### Create a Table

```swift
try connection.execute(sql: "CREATE TABLE t1(a,b);")
```

The created table *t1* has two columns, *a* and *b*.

### Insert Data

```swift
for i in 0..<5 {
    try connection.execute(sql: "INSERT INTO t1(a,b) VALUES (?,?);",
                           parameters: .int(2*i), .int(2*i+1))
}
```
SQL parameters are passed as a sequence or series of values.  Named parameters are also supported.

```swift
try connection.execute(sql: "INSERT INTO t1(a,b) VALUES (:a,:b);",
                       parameters: [":a": 100, ":b": 404])
```

### Insert Data Efficiently

Rather than parsing SQL each time a statement is executed, it is more efficient to prepare a statement and reuse it.

```swift
let statement = try connection.prepare(sql: "INSERT INTO t1(a,b) VALUES (?,?);")
for i in 0..<5 {
    try statement.bind(.int(2*i), .int(2*i+1))
    try statement.execute()
    try statement.reset()
    try statement.clearBindings()
}
```

### Fetch Data

The closure passed to `execute()` will be called with each result row.

```swift
try connection.execute(sql: "SELECT * FROM t1;") { row in
    let x = try row.get(.int, at: 0)
    let y = try row.optional(.int, at: 1)
}
```

*row* is a `Row` instance.

### Perform a Transaction

```swift
try connection.transaction { connection, command in
    // Do something with `connection`
}
```

Transactions are committed by default after the transaction closure returns.

To roll back a transaction instead, set `command` to `.rollback`:

```swift
try connection.transaction { connection, command in
    // If a condition occurs that prevents the transaction from committing:
    command = .rollback
}
```

A rollback is not considered an error condition unless execution of the rollback fails.

Transactions may also return a value:

```swift
let (command, value) = try connection.transaction { connection, command -> Int64 in
    // ... some long and complex sequence of database commands inserting a row 
    return connection.lastInsertRowid
}
```

`command` contains the result of the transaction (whether it was committed or rolled back), and `value` is the value returned from the transaction closure.

Database transactions may also be performed asynchronously using `ConnectionQueue`.

```swift
connectionQueue.asyncTransaction { connection, command in
    // Do something with `connection`
} completion: { result in
    switch result {
        case .success:
            // 🎉
        case .failure(let error):
            // Handle any errors that occurred
    }
}
```

### Custom SQL Functions

```swift
let rot13Mapping: [Character: Character] = [
    "A": "N", "B": "O", "C": "P", "D": "Q", "E": "R", "F": "S", "G": "T", "H": "U", "I": "V", "J": "W", "K": "X", "L": "Y", "M": "Z",
    "N": "A", "O": "B", "P": "C", "Q": "D", "R": "E", "S": "F", "T": "G", "U": "H", "V": "I", "W": "J", "X": "K", "Y": "L", "Z": "M",
    "a": "n", "b": "o", "c": "p", "d": "q", "e": "r", "f": "s", "g": "t", "h": "u", "i": "v", "j": "w", "k": "x", "l": "y", "m": "z",
    "n": "a", "o": "b", "p": "c", "q": "d", "r": "e", "s": "f", "t": "g", "u": "h", "v": "i", "w": "j", "x": "k", "y": "l", "z": "m"]

try connection.addFunction("rot13", arity: 1) { values in
    let value = values.first.unsafelyUnwrapped
    switch value {
        case .text(let t):
            return .text(String(t.map { rot13Mapping[$0] ?? $0 }))
        default:
            return value
    }
}
```

*rot13* can now be used just like any other [SQL function](https://www.sqlite.org/lang_corefunc.html).

```swift
let statement = try connection.prepare(sql: "INSERT INTO t1(a) VALUES (rot13(?));")
```

### Custom Collating Sequences

```swift
try connection.addCollation("localized_compare", { (lhs, rhs) -> ComparisonResult in
    lhs.localizedCompare(rhs)
})
```

*localized_compare* is now available as a [collating sequence](https://www.sqlite.org/c3ref/create_collation.html).

```swift
let statement = try connection.prepare(sql: "SELECT * FROM t1 ORDER BY a COLLATE localized_compare;")
```

## Combine

Pipeline provides a Combine publisher for SQLite query results, allowing you to write elegant and powerful data processing code.

```swift
// CREATE TABLE event(description TEXT NOT NULL, date REAL NOT NULL);
struct Event {
    let description: String
    let date: Date
}

let eventConverter = RowConverter<Event> { row in
    let description = try row.text(at: 0)
    let date = try row.get(.dateWithTimeIntervalSinceReferenceDate, at: 1)
    return Event(description: description, date: date)
}

let connection = try Connection()

let sevenDaysAgo = Date() - 7 * 24 * 60 * 60

let publisher = connection.rowPublisher(sql: "select what, when from event where when >= ?;") {
    try $0.bind(.timeIntervalSinceReferenceDate(sevenDaysAgo), toParameter: 1)
}

publisher
    .mapRows(eventConverter)
```

## Miscellaneous

### CSQLite

Pipeline uses [CSQLite](https://github.com/sbooth/CSQLite), a Swift package of the SQLite [amalgamation](https://sqlite.org/amalgamation.html) with the [carray](https://www.sqlite.org/carray.html), [decimal](https://sqlite.org/src/file/ext/misc/decimal.c), [ieee754](https://sqlite.org/src/file/ext/misc/ieee754.c), [series](https://sqlite.org/src/file/ext/misc/series.c), [sha3](https://sqlite.org/src/file/ext/misc/shathree.c), and [uuid](https://sqlite.org/src/file/ext/misc/uuid.c) extensions added, along with wrappers for C functions not easily usable from Swift.

### SQLite Build Options

For performance reasons CSQLite is built without pre-update hook support. Unfortunately there is no way using Swift Package Manager to expose [package features](https://forums.swift.org/t/my-swiftpm-wishlist-aka-proposal-proposals/35292) or build options, in this case the SQLite [pre-update hook](https://sqlite.org/c3ref/preupdate_count.html) and the [session](https://sqlite.org/sessionintro.html) extension. For this reason SQLite build options must be customized by changing to a local CSQLite package dependency and editing [CSQLite/Package.swift](https://github.com/sbooth/CSQLite/blob/main/Package.swift).

## License

Pipeline is released under the [MIT License](https://github.com/sbooth/Pipeline/blob/main/LICENSE.txt).
