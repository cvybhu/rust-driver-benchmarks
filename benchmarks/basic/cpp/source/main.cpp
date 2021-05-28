#include <atomic>
#include <cassandra.h>
#include <chrono>
#include <thread>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <utility>
#include <vector>

#include "config.hpp"
#include "semaphore.hpp"

// Global variables for easy access from callbacks
Config *config;
CassSession *session;
const CassPrepared *prepared_insert;
const CassPrepared *prepared_select;

std::atomic<int64_t> next_batch_start;
Semaphore *finished_semaphore;

void assert_future_ok(CassFuture *future, const char *message) {
    if (cass_future_error_code(future) == CASS_OK) {
        return;
    }

    const char *error_msg;
    size_t error_msg_len;
    cass_future_error_message(future, &error_msg, &error_msg_len);
    fprintf(stderr, "ERROR: %s: '%.*s'\n", message, (int)error_msg_len, error_msg);

    std::exit(1);
}

void connect() {
    CassCluster *cluster = cass_cluster_new();
    if (cass_cluster_set_queue_size_io(cluster, 2 * config->concurrency) != CASS_OK) {
        fprintf(stderr, "ERROR: Failed to set io queue size\n");
        std::exit(1);
    }

    if (cass_cluster_set_protocol_version(cluster, CASS_PROTOCOL_VERSION_V4) != CASS_OK) {
        fprintf(stderr, "ERROR: Failed to set protocol version\n");
        std::exit(1);
    }

    session = cass_session_new();

    cass_cluster_set_contact_points(cluster, config->node_address);
    CassFuture *connect_future = cass_session_connect(session, cluster);
    cass_future_wait(connect_future);

    assert_future_ok(connect_future, "Unable to connect");
    cass_future_free(connect_future);

    // Cluster gets leaked, it's ok this is just a benchmark
}

void run_simple_query(const char *query) {
    CassStatement *statement = cass_statement_new(query, 0);

    CassFuture *result_future = cass_session_execute(session, statement);
    cass_future_wait(result_future);

    assert_future_ok(result_future, "Simple query failed");
    cass_future_free(result_future);
}

const CassPrepared *prepare_query(const char *query) {
    CassFuture *prepare_future = cass_session_prepare(session, query);
    cass_future_wait(prepare_future);
    assert_future_ok(prepare_future, "Preparing failed");

    const CassPrepared *result = cass_future_get_prepared(prepare_future);
    cass_future_free(prepare_future);

    return result;
}

struct CallbackData {
    int64_t cur_pk;
    int64_t end_pk;
};

void run_concurrent_task(CallbackData *);
void select_callback(CassFuture *, void *);

void insert_callback(CassFuture *insert_future, void *data) {
    CallbackData *callback_data = (CallbackData *)data;

    assert_future_ok(insert_future, "Insert failed");

    if (config->workload == Workload::Inserts) {
        // Ok all done, start next insert
        callback_data->cur_pk += 1;
        run_concurrent_task(callback_data);
        return;
    }

    // In case of Workload::Mixed we need to perform a select after insert
    CassStatement *statement = cass_prepared_bind(prepared_select);
    cass_statement_bind_int64(statement, 0, callback_data->cur_pk);

    CassFuture *select_future = cass_session_execute(session, statement);
    cass_future_set_callback(select_future, select_callback, (void *)callback_data);

    cass_statement_free(statement);
    cass_future_free(select_future);
}

void select_callback(CassFuture *select_future, void *data) {
    CallbackData *callback_data = (CallbackData *)data;

    assert_future_ok(select_future, "Select failed");

    // Ensure that returned row contains 2*pk and 3*pk
    const CassResult *result = cass_future_get_result(select_future);
    CassIterator *res_iter = cass_iterator_from_result(result);

    if (!cass_iterator_next(res_iter)) {
        fprintf(stderr, "ERROR: Select did not return any rows!\n");
        std::exit(1);
    }

    const CassRow *row = cass_iterator_get_row(res_iter);

    int64_t pk = callback_data->cur_pk;
    int64_t v1 = 0;
    int64_t v2 = 0;

    cass_value_get_int64(cass_row_get_column(row, 0), &v1);
    cass_value_get_int64(cass_row_get_column(row, 1), &v2);

    if (v1 != 2 * callback_data->cur_pk || v2 != 3 * callback_data->cur_pk) {
        fprintf(stderr, "ERROR: Invalid row: (pk, v1, v2) = (%ld, %ld, %ld)\n", pk, v1, v2);
        std::exit(1);
    }

    cass_iterator_free(res_iter);
    cass_result_free(result);

    // Continue running concurrent task
    callback_data->cur_pk += 1;
    run_concurrent_task(callback_data);
}

void run_concurrent_task(CallbackData *callback_data) {
    // If current batch has finished we need to acquire a new one
    if (callback_data->cur_pk >= callback_data->end_pk) {
        int64_t new_batch_start = next_batch_start.fetch_add(config->batch_size);

        if (new_batch_start >= config->tasks) {
            // No more work to do
            finished_semaphore->free_permit();
            return;
        }

        callback_data->cur_pk = new_batch_start;
        callback_data->end_pk = std::min(new_batch_start + config->batch_size, config->tasks);
    }

    if (config->workload == Workload::Inserts || config->workload == Workload::Mixed) {
        // Perform an insert
        CassStatement *statement = cass_prepared_bind(prepared_insert);
        cass_statement_bind_int64(statement, 0, callback_data->cur_pk);
        cass_statement_bind_int64(statement, 1, 2 * callback_data->cur_pk);
        cass_statement_bind_int64(statement, 2, 3 * callback_data->cur_pk);

        CassFuture *insert_future = cass_session_execute(session, statement);
        cass_future_set_callback(insert_future, insert_callback, (void *)callback_data);

        cass_statement_free(statement);
        cass_future_free(insert_future);
    }

    if (config->workload == Workload::Selects) {
        // Perform a select
        CassStatement *statement = cass_prepared_bind(prepared_select);
        cass_statement_bind_int64(statement, 0, callback_data->cur_pk);

        CassFuture *select_future = cass_session_execute(session, statement);
        cass_future_set_callback(select_future, select_callback, (void *)callback_data);

        cass_statement_free(statement);
        cass_future_free(select_future);
    }
}

void prepare_keyspace_and_table();
void prepare_selects_benchmark();

int main(int argc, const char *argv[]) {
    std::cout << "Starting cpp-driver benchmark!\n\n";

    // Parse config
    Config the_config(argc, argv);
    config = &the_config;

    std::cout << "Benchmark configuration:\n";
    config->print();

    // Connect to the cluster
    connect();

    // Prepare before the benchmark
    if (!config->no_prepare) {
        prepare_keyspace_and_table();
    }

    prepared_insert = prepare_query("INSERT INTO benchks.benchtab (pk, v1, v2) VALUES(?, ?, ?)");
    prepared_select = prepare_query("SELECT v1, v2 FROM benchks.benchtab WHERE pk = ?");

    if (config->workload == Workload::Selects && !config->no_prepare) {
        prepare_selects_benchmark();
    }

    // Start concurrent tasks
    std::vector<CallbackData> callbacks_data(config->concurrency);
    Semaphore fin_semaphore(config->concurrency);
    finished_semaphore = &fin_semaphore;
    next_batch_start = 0;

    std::cout << "\nStarting the benchmark\n";
    auto start_time = std::chrono::high_resolution_clock::now();

    for (int64_t c = 0; c < config->concurrency; c++) {
        callbacks_data[c] = CallbackData{.cur_pk = 0, .end_pk = 0};

        finished_semaphore->acquire_permit();
        run_concurrent_task(&callbacks_data[c]);
    }

    // Wait for concurrent tasks to finish
    for (int64_t c = 0; c < config->concurrency; c++) {
        finished_semaphore->acquire_permit();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "Finished\n\nBenchmark time: " << millis.count() << " ms\n";

    cass_session_free(session);
    return 0;
}

void prepare_keyspace_and_table() {
    run_simple_query("DROP KEYSPACE IF EXISTS benchks");

    std::this_thread::sleep_for(std::chrono::seconds(4)); // Await schema agreement

    run_simple_query("CREATE KEYSPACE IF NOT EXISTS benchks WITH REPLICATION = {'class' "
                        ": 'SimpleStrategy', 'replication_factor' : 1}");

    std::this_thread::sleep_for(std::chrono::seconds(4));

    run_simple_query("CREATE TABLE IF NOT EXISTS benchks.benchtab (pk "
                        "bigint PRIMARY KEY, v1 bigint, v2 bigint)");

    std::this_thread::sleep_for(std::chrono::seconds(4));
}

void prepare_selects_benchmark() {
    // Before performing selects we have to insert values to select
    std::cout << "Preparing a selects benchmark (inserting values)...\n";

    // Pretend the workload is Inserts and run concurrent tasks
    config->workload = Workload::Inserts;

    std::vector<CallbackData> callbacks_data(config->concurrency);
    Semaphore fin_semaphore(config->concurrency);
    finished_semaphore = &fin_semaphore;
    next_batch_start = 0;

    for (int64_t c = 0; c < config->concurrency; c++) {
        callbacks_data[c] = CallbackData{.cur_pk = 0, .end_pk = 0};

        fin_semaphore.acquire_permit();
        run_concurrent_task(&callbacks_data[c]);
    }

    // Wait for concurrent tasks to finish
    for (int64_t c = 0; c < config->concurrency; c++) {
        fin_semaphore.acquire_permit();
    }

    // Restore previous workload
    config->workload = Workload::Selects;
}