import ConnectionPool

class ClosureConnection: Connection {
    typealias Closer = (Void) throws -> Void
    private let closer: Closer

    init(closer: @escaping Closer) {
        self.closer = closer
    }

    func close() throws {
        try self.closer()
    }
}

class ClosureConnectionFactory: ConnectionFactory {
    typealias ConnectionMaker = (Void) throws -> Connection
    private let connectionMaker: ConnectionMaker

    init(connectionMaker: @escaping ConnectionMaker) {
        self.connectionMaker = connectionMaker
    }

    convenience init(connectionCloser: @escaping ClosureConnection.Closer) {
        self.init(connectionMaker: {
            return ClosureConnection(closer: connectionCloser)
        })
    }

    func connection() throws -> Connection {
        return try self.connectionMaker()
    }
}
