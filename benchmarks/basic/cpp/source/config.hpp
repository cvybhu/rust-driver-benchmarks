#ifndef CONFIG_HPP
#define CONFIG_HPP

#include <cstring>
#include <iostream>
#include <stdio.h>

enum class Workload {
    Inserts,
    Selects,
    Mixed,
};

inline void print_usage();

struct Config {
    const char *node_address;
    Workload workload;
    int64_t tasks;
    int64_t concurrency;
    int64_t batch_size;
    bool no_prepare;

    Config(int argc, const char *argv[]) {
        node_address = "scylla";
        workload = Workload::Mixed;
        tasks = 1000 * 1000;
        concurrency = 256;
        no_prepare = false;

        for (int a = 1; a < argc;) {
            // --help
            if (strcmp(argv[a], "-h") == 0 || strcmp(argv[a], "--help") == 0) {
                print_usage();
                std::exit(0);
            }

            // --no-prepare
            if (strcmp(argv[a], "-n") == 0 || strcmp(argv[a], "--no-prepare") == 0) {
                no_prepare = true;
                a += 1;
                continue;
            }

            if (a + 1 >= argc) {
                fprintf(stderr, "ERROR: No value specified for argument %s!\n", argv[a]);
                std::exit(1);
            }

            // --address
            if (strcmp(argv[a], "-a") == 0 || strcmp(argv[a], "--address") == 0) {
                node_address = argv[a + 1];
                a += 2;
                continue;
            }

            // --workload
            if (strcmp(argv[a], "-w") == 0 || strcmp(argv[a], "--workload") == 0) {
                if (strcmp(argv[a + 1], "inserts") == 0) {
                    workload = Workload::Inserts;
                } else if (strcmp(argv[a + 1], "selects") == 0) {
                    workload = Workload::Selects;
                } else if (strcmp(argv[a + 1], "mixed") == 0) {
                    workload = Workload::Mixed;
                } else {
                    fprintf(stderr,
                            "ERROR: Invalid workload specified: %s. (Allowed values: inserts, "
                            "selects, mixed)\n",
                            argv[a + 1]);
                    
                    std::exit(1);
                }

                a += 2;
                continue;
            }

            // --tasks
            if (strcmp(argv[a], "-t") == 0 || strcmp(argv[a], "--tasks") == 0) {
                tasks = std::strtoll(argv[a + 1], nullptr, 10);

                a += 2;
                continue;
            }

            // --concurrency
            if (strcmp(argv[a], "-c") == 0 || strcmp(argv[a], "--concurrency") == 0) {
                concurrency = std::strtoll(argv[a + 1], nullptr, 10);

                a += 2;
                continue;
            }

            fprintf(stderr, "Unkown argument: %s, see --help for usage\n", argv[a]);
            std::exit(1);
        }

        batch_size = 256;

        if (tasks/batch_size < concurrency) {
            batch_size = std::max((int64_t)1, tasks / concurrency);
        }
    }

    void print() {
        std::cout << "Config:\n";
        std::cout << "    address: " << node_address << "\n";
        std::cout << "    workload: ";

        switch (workload) {
        case Workload::Inserts:
            std::cout << "Inserts\n";
            break;

        case Workload::Selects:
            std::cout << "Selects\n";
            break;

        case Workload::Mixed:
            std::cout << "Mixed\n";
            break;
        }

        std::cout << "    tasks: " << tasks << "\n";
        std::cout << "    concurrency: " << concurrency << "\n";
        std::cout << "    no_prepare: " << (no_prepare ? "true" : "false") << "\n";

        std::cout << std::endl;
    }
};

void print_usage() {}

#endif