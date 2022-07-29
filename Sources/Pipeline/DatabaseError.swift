//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation

/// An error supplying a message and description.
public struct DatabaseError: Error {
	/// A brief message describing the error.
	public let message: String

	/// A more detailed description of the error's cause.
	public let details: String?

	/// Creates an error with the given message and details.
	///
	/// - parameter message: A brief message describing the error.
	/// - parameter details: A description of the error's cause.
	public init(message: String, details: String?) {
		self.message = message
		self.details = details
	}
}

extension DatabaseError {
	/// Creates an error with the given message.
	///
	/// - parameter message: A brief message describing the error.
	public init(_ message: String) {
		self.init(message: message, details: nil)
	}
}

extension DatabaseError: CustomStringConvertible {
	public var description: String {
		if let details = details {
			return "\(message): \(details)"
		} else {
			return message
		}
	}
}

extension DatabaseError: LocalizedError {
	public var errorDescription: String? {
		return message
	}

	public var failureReason: String? {
		return details
	}
}
