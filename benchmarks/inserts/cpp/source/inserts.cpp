#include <cassandra.h>
#include <cstdio>
#include <cstdlib>
#include <utility>
#include <chrono>
#include <vector>
#include <iostream>

// Number of inserts to be performed
const int num_of_inserts = 10 * 1000;

void assert_future_ok(CassFuture* future, const char* message) {
    if (cass_future_error_code(future) == CASS_OK) {
        return;
    }

    const char* error_msg;
    size_t error_msg_len;
    cass_future_error_message(future, &error_msg, &error_msg_len);
    fprintf(stderr, "ERROR: %s: '%.*s'\n", message, (int)error_msg_len, error_msg);

    // Too lazy exit cleanly, it's just a benchmark
    std::exit(1); 
}

CassSession* connect() {
    CassCluster* cluster = cass_cluster_new();
    if(cass_cluster_set_queue_size_io(cluster, 2 * num_of_inserts) != CASS_OK) {
        fprintf(stderr, "ERROR: Failed to set io queue size");
        std::exit(1);
    }

    CassSession* session = cass_session_new();

    cass_cluster_set_contact_points(cluster, "scylla");
    CassFuture* connect_future = cass_session_connect(session, cluster);
    cass_future_wait(connect_future);

    assert_future_ok(connect_future, "Unable to connect");
    cass_future_free(connect_future);

    // Cluster gets leaked, oh no
    return session;
}

void run_simple_query(CassSession* session, const char* query) {
    CassStatement* statement = cass_statement_new(query, 0);

    CassFuture* result_future = cass_session_execute(session, statement);
    cass_future_wait(result_future);

    assert_future_ok(result_future, "Simple query failed");

    cass_future_free(result_future);
}

const CassPrepared* prepare_insert_query(CassSession* session) {
    const char* query = "INSERT INTO ks.benchtab (a) VALUES(?)";

    CassFuture* prepare_future = cass_session_prepare(session, query);
    cass_future_wait(prepare_future);
    assert_future_ok(prepare_future, "Preparing the insert statement failed");

    const CassPrepared* result = cass_future_get_prepared(prepare_future);
    cass_future_free(prepare_future);

    return result;
}

int main() {
    // Connect to the cluster
    CassSession* session = connect();   

    // Create a simple keyspace and table
    run_simple_query(session, "CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}");
    run_simple_query(session, "CREATE TABLE IF NOT EXISTS ks.benchtab (a int primary key)");

    const CassPrepared* prepared_insert = prepare_insert_query(session);

    // Start the benchmark
    std::cout << "Starting benchmark (" << num_of_inserts << " inserts)\n";

    std::vector<CassStatement*> statements;
    std::vector<CassFuture*> futures;

    futures.reserve(num_of_inserts);
    statements.reserve(num_of_inserts);

    auto start_time = std::chrono::high_resolution_clock::now();

    for(int to_insert = 0; to_insert < num_of_inserts; to_insert++) {
        CassStatement* statement = cass_prepared_bind(prepared_insert);
        cass_statement_bind_int32(statement, 0, to_insert);

        CassFuture* execute_future = cass_session_execute(session, statement);

        statements.push_back(statement);
        futures.push_back(execute_future);
    }

    for (auto&& future : futures) {
        cass_future_wait(future);
        assert_future_ok(future, "Insert query failed");
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "Benchmark took: " << millis.count() << "ms\n";

    for (auto&& future : futures) {
        cass_future_free(future);
    }

    for (auto&& statement : statements) {
        cass_statement_free(statement);
    }

    return 0;
}