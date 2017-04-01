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

func closureConnectionFactory(connectionCloser: @escaping ClosureConnection.Closer) throws -> ClosureConnection {
    return ClosureConnection(closer: connectionCloser)
}

func closureConnectionFactory() throws -> ClosureConnection {
    return ClosureConnection(closer: {})
}
