import XCTest
import ConnectionPool

class ReserveOrWaitTimeoutTests: XCTestCase {
    func test_wait_timeout() throws {
        let pool = DispatchPool<ClosureConnection>(connectionFactory: closureConnectionFactory, maxConnections: 1, maxIdleConnections: nil)
        let expectation = self.expectation(description: "Unblocked")
        DispatchQueue.global().async {
            do {
                let c1 = try pool.reserveOrWait(timeout: nil)
                XCTAssertNotNil(c1)
                let d1 = Date()
                let c2 = try pool.reserveOrWait(timeout: 1)
                XCTAssertNil(c2)
                let d2 = Date()
                let duration = d2.timeIntervalSince(d1)
                XCTAssertGreaterThan(duration, 1.0)
                XCTAssertLessThan(duration, 1.1)
                expectation.fulfill()
            } catch {
                XCTFail("Unexpected error \(error)")
            }
        }

        self.waitForExpectations(timeout: 1.3, handler: nil)
    }

    func test_wait_timeout_interrupted_by_free() throws {
        let pool = DispatchPool<ClosureConnection>(connectionFactory: closureConnectionFactory, maxConnections: 1, maxIdleConnections: nil)
        let expectation = self.expectation(description: "Unblocked")
        DispatchQueue.global().async {
            do {
                let c1 = try pool.reserveOrWait(timeout: nil)
                XCTAssertNotNil(c1)
                DispatchQueue.global().asyncAfter(deadline: .now() + 0.5) {
                    do {
                        try pool.free(c1!)
                    } catch {
                        XCTFail("Unexpected error \(error) when freeing connection")
                    }
                }

                let d1 = Date()
                let c2 = try pool.reserveOrWait(timeout: 1)
                XCTAssertNotNil(c2)
                let d2 = Date()
                let duration = d2.timeIntervalSince(d1)
                XCTAssertGreaterThan(duration, 0.5)
                XCTAssertLessThan(duration, 0.7)
                expectation.fulfill()
            } catch {
                XCTFail("Unexpected error \(error)")
            }
        }

        self.waitForExpectations(timeout: 0.8, handler: nil)
    }

    func test_wait_timeout_first_interrupted_by_free_second_times_out() throws {
        let pool = DispatchPool<ClosureConnection>(connectionFactory: closureConnectionFactory, maxConnections: 1, maxIdleConnections: nil)
        let expectation2 = self.expectation(description: "Unblocked 2")
        let expectation3 = self.expectation(description: "Timed out 3")
        DispatchQueue.global().async {
            do {
                let c1 = try pool.reserveOrWait(timeout: nil)
                XCTAssertNotNil(c1)
                DispatchQueue.global().asyncAfter(deadline: .now() + 0.5) {
                    do {
                        try pool.free(c1!)
                    } catch {
                        XCTFail("Unexpected error \(error) when freeing connection")
                    }
                }

                DispatchQueue.global().async {
                    DispatchQueue.global().async {
                        let d1 = Date()
                        do {
                            let c3 = try pool.reserveOrWait(timeout: 1)
                            XCTAssertNil(c3)
                        } catch {
                            XCTFail("Unexpected error \(error) waiting for c3")
                        }
                        let d2 = Date()
                        let duration = d2.timeIntervalSince(d1)
                        XCTAssertGreaterThan(duration, 1.0)
                        XCTAssertLessThan(duration, 1.1)
                        expectation3.fulfill()
                    }

                    do {
                        let c2 = try pool.reserveOrWait(timeout: 1)
                        XCTAssertNotNil(c2)
                        expectation2.fulfill()
                    } catch {
                        XCTFail("Unexpected error \(error) waiting for c2")
                    }
                }

            } catch {
                XCTFail("Unexpected error \(error)")
            }
        }

        self.waitForExpectations(timeout: 1.1, handler: nil)
    }

    func test_wait_timeout_first_interrupted_by_free_second_interrupted_by_free() throws {
        let pool = DispatchPool<ClosureConnection>(connectionFactory: closureConnectionFactory, maxConnections: 1, maxIdleConnections: nil)
        let expectation2 = self.expectation(description: "Unblocked 2")
        let expectation3 = self.expectation(description: "Timed out 3")
        DispatchQueue.global().async {
            do {
                let c1 = try pool.reserveOrWait(timeout: nil)
                XCTAssertNotNil(c1)
                DispatchQueue.global().asyncAfter(deadline: .now() + 0.5) {
                    do {
                        try pool.free(c1!)
                    } catch {
                        XCTFail("Unexpected error \(error) when freeing connection")
                    }
                }

                DispatchQueue.global().async {
                    DispatchQueue.global().async {
                        let d1 = Date()
                        do {
                            let c3 = try pool.reserveOrWait(timeout: 1)
                            XCTAssertNotNil(c3)
                        } catch {
                            XCTFail("Unexpected error \(error) waiting for c3")
                        }
                        let d2 = Date()
                        let duration = d2.timeIntervalSince(d1)
                        XCTAssertLessThan(duration, 1.0)
                        expectation3.fulfill()
                    }

                    do {
                        let c2 = try pool.reserveOrWait(timeout: 1)
                        XCTAssertNotNil(c2)
                        try pool.free(c2!)
                        expectation2.fulfill()
                    } catch {
                        XCTFail("Unexpected error \(error) waiting for c2")
                    }
                }

            } catch {
                XCTFail("Unexpected error \(error)")
            }
        }

        self.waitForExpectations(timeout: 1.1, handler: nil)
    }
}
