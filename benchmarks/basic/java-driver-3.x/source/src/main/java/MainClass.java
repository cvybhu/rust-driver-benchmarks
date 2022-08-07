import com.datastax.driver.core.*;

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

        AtomicLong nextBatchStart = new AtomicLong(0);

        executor = Executors.newFixedThreadPool((int) config.concurrency);

        System.out.println("Starting the benchmark");

        benchmarkStart = System.nanoTime();
        ArrayList<Future<?>> arr = new ArrayList<>();
        for (int i = 0; i < config.concurrency; i++) {
            arr.add(
                    executor.submit(() -> {
                        PreparedStatement insertQ = session.prepare(INSERT_STRING);
                        PreparedStatement selectQ = session.prepare(SELECT_STRING);
                        while (true) {

                            long curBatchStart = nextBatchStart.addAndGet(config.batch_size);
                            if (curBatchStart >= config.tasks) {
                                break;
                            }

                            long curBatchEnd = Math.min(curBatchStart + config.batch_size, config.tasks);

                            for (long pk = curBatchStart; pk < curBatchEnd; pk++) {
                                if (config.workload.equals(Config.Workload.Inserts) || config.workload.equals(Config.Workload.Mixed)) {
                                    session.execute(insertQ.bind(pk, 2L * pk, 3L * pk));
                                }

                                if (config.workload.equals(Config.Workload.Selects) || config.workload.equals(Config.Workload.Mixed)) {
                                    ResultSet rs = session.execute(selectQ.bind(pk));
                                    Row r = rs.one();
                                    if ((r.getLong("v1") != 2 * pk) || (r.getLong("v2") != 3 * pk)) {
                                        throw new RuntimeException(String.format("Received incorrect data. " + "Expected: (%s, %s, %s). " + "Received: (%s, %s ,%s).", pk, 2 * pk, 3 * pk, r.getInt("pk"), r.getInt("v1"), r.getInt("v2")));
                                    }
                                }
                            }
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
        executor = Executors.newFixedThreadPool((int) config.concurrency);

        ArrayList<Future<?>> arr = new ArrayList<>();
        try {
            for (int i = 0; i < config.concurrency; i++) {
                arr.add(executor.submit(() -> {
                    PreparedStatement insertQ = session.prepare(INSERT_STRING);
                    while (true) {
                        long curBatchStart = nextBatchStart.addAndGet(config.batch_size);
                        if (curBatchStart >= config.tasks) {
                            break;
                        }
                        long curBatchEnd = Math.min(curBatchStart + config.batch_size, config.tasks);
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


