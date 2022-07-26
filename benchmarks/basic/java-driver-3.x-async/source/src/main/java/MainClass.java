import com.datastax.driver.core.*;
import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class MainClass {

    private static Cluster cluster;
    private static Config config;

    private static ExecutorService executor;
    private static final String INSERT_STRING = "INSERT INTO benchks.benchtab (pk, v1, v2) VALUES(?, ?, ?)";
    private static final String SELECT_STRING = "SELECT v1, v2 FROM benchks.benchtab WHERE pk = ?";

    private static long benchmarkStart;

    private static long benchmarkEnd;

    public static void main(String[] args) throws InterruptedException, ExecutionException {

        config = new Config(args);
        System.out.println("Parsed config: ");
        System.out.println(config.toString());

        cluster = Cluster.builder().addContactPoints(config.node_addresses).withProtocolVersion(ProtocolVersion.V4).build();
        cluster.getConfiguration().getPoolingOptions().setMaxQueueSize((int) Math.max(2048, 2 * config.concurrency));
        Session session = cluster.connect();

        prepareKeyspaceAndTable(session);

        if (!config.dont_prepare) {
            prepareKeyspaceAndTable(session);

            if (config.workload.equals(Config.Workload.Selects)) {
                prepareSelectsBenchmark(session);
            }
        }

        AtomicLong requestsSent = new AtomicLong(0);

        executor = Executors.newFixedThreadPool(config.threads);

        System.out.println("Starting the benchmark");

        benchmarkStart = System.nanoTime();
        ArrayList<Future<?>> arr = new ArrayList<>();
        for (int i = 0; i < config.threads; i++) {
            arr.add(
                    executor.submit(() -> {
                        int permits = (int) (config.concurrency / config.threads);
                        Semaphore semaphore = new Semaphore(permits);
                        PreparedStatement insertQ = session.prepare(INSERT_STRING);
                        PreparedStatement selectQ = session.prepare(SELECT_STRING);
                        long pk;
                        while ((pk = requestsSent.incrementAndGet()) < config.tasks) {
                            try {
                                semaphore.acquire();
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            if (config.workload.equals(Config.Workload.Inserts) || config.workload.equals(Config.Workload.Mixed)) {
                                ListenableFuture<ResultSet> resultSetA = session.executeAsync(insertQ.bind(pk, 2L * pk, 3L * pk));
                                ListenableFuture<ResultSet> resultSetB;

                                long finalPk = pk;
                                if (config.workload.equals(Config.Workload.Selects) || config.workload.equals(Config.Workload.Mixed)) {
                                    // Chain with select depending on workload
                                    resultSetB = Futures.transform(resultSetA,
                                            new AsyncFunction<ResultSet, ResultSet>() {
                                                public ListenableFuture<ResultSet> apply(ResultSet rs) throws Exception {
                                                    return session.executeAsync(selectQ.bind(finalPk));
                                                }
                                            });
                                } else {
                                    resultSetB = resultSetA;
                                }
                                ListenableFuture<Boolean> result;
                                // Verify returned data depending on workload and return the permit
                                if (config.workload.equals(Config.Workload.Selects) || config.workload.equals(Config.Workload.Mixed)) {
                                    result = Futures.transform(resultSetB,
                                            new Function<ResultSet, Boolean>() {
                                                public Boolean apply(ResultSet rs) {
                                                    semaphore.release();
                                                    Row r = rs.one();
                                                    if ((r.getLong("v1") != 2 * finalPk) || (r.getLong("v2") != 3 * finalPk)) {
                                                        throw new RuntimeException(String.format("Received incorrect data. " + "Expected: (%s, %s, %s). " + "Received: (%s, %s ,%s).", finalPk, 2 * finalPk, 3 * finalPk, r.getInt("pk"), r.getInt("v1"), r.getInt("v2")));
                                                    }
                                                    return true;
                                                }
                                            });

                                }
                                else {
                                    result = Futures.transform(resultSetB,
                                            new Function<ResultSet, Boolean>() {
                                                public Boolean apply(ResultSet rs) {
                                                    semaphore.release();
                                                    return true;
                                                }
                                            });
                                }
                            }
                        }
                        try {
                            //possible only if all futures ended successfully
                            semaphore.acquire(permits);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }));
        }


        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        benchmarkEnd = System.nanoTime();
        for (Future<?> f : arr) {
            f.get(); // make sure nothing has thrown
        }
        System.out.println(String.format("Finished\nBenchmark time: %d ms\n", (benchmarkEnd - benchmarkStart) / 1_000_000));

        session.close();
        if (cluster != null) cluster.close();
    }

    static void prepareKeyspaceAndTable(Session session) {
        session.execute("DROP KEYSPACE IF EXISTS benchks");
        session.execute("CREATE KEYSPACE IF NOT EXISTS benchks WITH REPLICATION = {'class' " + ": 'SimpleStrategy', 'replication_factor' : 1}");
        session.execute("CREATE TABLE IF NOT EXISTS benchks.benchtab (pk " + "bigint PRIMARY KEY, v1 bigint, v2 bigint)");
        if (!cluster.getMetadata().checkSchemaAgreement()) {
            throw new RuntimeException("Schema not in agreement after preparing keyspace and table.");
        }
    }

    private static void prepareSelectsBenchmark(Session session) throws InterruptedException, ExecutionException {
        System.out.println("Preparing a selects benchmark (inserting values)...");

        AtomicLong nextBatchStart = new AtomicLong(0);
        executor = Executors.newFixedThreadPool((int) config.threads);

        ArrayList<Future<?>> arr = new ArrayList<>();
        try {
            for (int i = 0; i < config.concurrency; i++) {
                arr.add(executor.submit(() -> {
                    PreparedStatement insertQ = session.prepare(INSERT_STRING);
                    while (true) {
                        long curBatchStart = nextBatchStart.addAndGet(config.tasks / config.concurrency);
                        if (curBatchStart >= config.tasks) {
                            break;
                        }
                        long curBatchEnd = Math.min(curBatchStart + (config.tasks / config.concurrency), config.tasks);
                        for (long pk = curBatchStart; pk < curBatchEnd; pk++) {
                            session.execute(insertQ.bind(pk, 2L * pk, 3L * pk));
                        }
                    }
                }));
            }
        } finally {
            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            for (Future<?> f : arr) {
                f.get(); // make sure nothing has thrown
            }
        }
    }
}


