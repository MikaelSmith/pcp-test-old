#include <pcp-test/test_connection.hpp>
#include <pcp-test/test_connection_parameters.hpp>
#include <pcp-test/util.hpp>
#include <pcp-test/errors.hpp>
#include <pcp-test/client_configuration.hpp>
#include <pcp-test/random.hpp>

#include <cpp-pcp-client/connector/errors.hpp>

#include <leatherman/logging/logging.hpp>

#include <leatherman/json_container/json_container.hpp>

#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>

#include <boost/format.hpp>

#include <boost/nowide/iostream.hpp>

#include <algorithm>
#include <math.h>
#include <functional>  // std::reference_wrapper

// NOTE(ale): boost::future/async have different semantics than std::
#include <thread>
#include <future>

namespace pcp_test {

namespace lth_jc   = leatherman::json_container;
namespace conn_par = pcp_test::connection_test_parameters;
namespace fs       = boost::filesystem;

void run_connection_test(const application_options& a_o)
{
    connection_test test {a_o};
    test.start();
}

static std::string normalizeTimeInterval(uint32_t duration_ms)
{
    auto min = duration_ms / 60000;
    auto s   = (duration_ms - min * 60000) / 1000;
    auto ms  = duration_ms % 1000;

    if (min > 0)
        return (boost::format("%1% min %2% s") % min % s).str();

    if (s > 0)
        return (boost::format("%1%.%2% s") % s % ms).str();

    return (boost::format("%1% ms") % ms).str();
}

//
// connection_test_run
//

static constexpr int DEFAULT_INTER_ENDPOINT_PAUSE_RNG_SEED {1};

connection_test_run::connection_test_run(const application_options& a_o)
    : endpoints_increment_ {
        a_o.connection_test_parameters.get<int>(conn_par::ENDPOINTS_INCREMENT)},
      concurrency_increment_ {
        a_o.connection_test_parameters.get<int>(conn_par::CONCURRENCY_INCREMENT)},
      endpoint_timeout_ms_ {
        a_o.connection_test_parameters.get<int>(conn_par::WS_CONNECTION_TIMEOUT_MS)
        + 1000 * a_o.connection_test_parameters.get<int>(conn_par::ASSOCIATION_TIMEOUT_S)},
      idx {1},
      num_endpoints {a_o.connection_test_parameters.get<int>(conn_par::NUM_ENDPOINTS)},
      concurrency {a_o.connection_test_parameters.get<int>(conn_par::CONCURRENCY)},
      rng_seed {
        a_o.connection_test_parameters.includes(conn_par::INTER_ENDPOINT_PAUSE_RNG_SEED)
        ? a_o.connection_test_parameters.get<int>(conn_par::INTER_ENDPOINT_PAUSE_RNG_SEED)
        : DEFAULT_INTER_ENDPOINT_PAUSE_RNG_SEED},
      total_endpoint_timeout_ms {endpoint_timeout_ms_ * num_endpoints}
{
}

connection_test_run& connection_test_run::operator++()
{
    idx++;
    num_endpoints += endpoints_increment_;
    concurrency   += concurrency_increment_;
    rng_seed++;
    total_endpoint_timeout_ms += endpoint_timeout_ms_ * endpoints_increment_;

    return *this;
}

std::string connection_test_run::to_string() const
{
    return (boost::format("run %1%: %2% concurrent sets of %3% endpoints")
            % idx % concurrency % num_endpoints).str();
}

//
// connection_test_result
//

connection_test_result::connection_test_result(const connection_test_run& run)
    : num_endpoints {run.num_endpoints},
      concurrency {run.concurrency},
      num_failures {0},
      duration_ms {0},
      conn_stats {},
      start {std::chrono::high_resolution_clock::now()},
      completion {}
{
}

void connection_test_result::set_completion()
{
    completion = std::chrono::high_resolution_clock::now();
    duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       completion - start).count();
}

std::ostream & operator<< (std::ostream& out, const connection_test_result& r)
{
    auto tot_connections = r.num_endpoints * r.concurrency;
    if (r.num_failures) {
        out << util::red("  [FAILURE]  ") << r.num_failures
            << " connection failures out of "
            << tot_connections << " connection attempts";
    } else {
        out << util::green("  [SUCCESS]  ") << tot_connections
            << " successful connections";
    }

    out << " in " << normalizeTimeInterval(r.duration_ms);

    return out;
}

std::ofstream & operator<< (boost::nowide::ofstream& out,
                            const connection_test_result& r)
{
    out << r.num_endpoints << ","
        << r.concurrency << ","
        << r.num_failures << ","
        << r.duration_ms;

    return out;
}

//
// connection_test
//

static constexpr uint32_t DEFAULT_WS_CONNECTION_TIMEOUT_MS {1500};
static constexpr uint32_t DEFAULT_WS_CONNECTION_CHECK_INTERVAL_S {15};
static constexpr bool DEFAULT_RANDOMIZE_PAUSE {false};

connection_test::connection_test(const application_options& a_o)
    : app_opt_(a_o),
      num_runs_ {app_opt_.connection_test_parameters.get<int>(conn_par::NUM_RUNS)},
      inter_run_pause_ms_ {static_cast<unsigned int>(
            app_opt_.connection_test_parameters.get<int>(conn_par::INTER_RUN_PAUSE_MS))},
      inter_endpoint_pause_ms_ {static_cast<unsigned int>(
            app_opt_.connection_test_parameters.get<int>(conn_par::INTER_ENDPOINT_PAUSE_MS))},
      randomize_pause_ {
            app_opt_.connection_test_parameters.includes(conn_par::RANDOMIZE_INTER_ENDPOINT_PAUSE)
            ? app_opt_.connection_test_parameters.get<bool>(conn_par::RANDOMIZE_INTER_ENDPOINT_PAUSE)
            : DEFAULT_RANDOMIZE_PAUSE},
      mean_connection_rate_Hz_ {1000.0 / inter_endpoint_pause_ms_},
      ws_connection_timeout_ms_ {
            app_opt_.connection_test_parameters.includes(conn_par::WS_CONNECTION_TIMEOUT_MS)
            ? static_cast<unsigned int>(
                app_opt_.connection_test_parameters.get<int>(conn_par::WS_CONNECTION_TIMEOUT_MS))
            : DEFAULT_WS_CONNECTION_TIMEOUT_MS},
      ws_connection_check_interval_s_ {
            app_opt_.connection_test_parameters.includes(conn_par::WS_CONNECTION_CHECK_INTERVAL_S)
            ? static_cast<unsigned int>(
                app_opt_.connection_test_parameters.get<int>(conn_par::WS_CONNECTION_CHECK_INTERVAL_S))
            : DEFAULT_WS_CONNECTION_CHECK_INTERVAL_S},
      association_timeout_s_ {
            app_opt_.connection_test_parameters.includes(conn_par::ASSOCIATION_TIMEOUT_S)
            ? static_cast<unsigned int>(
                app_opt_.connection_test_parameters.get<int>(conn_par::ASSOCIATION_TIMEOUT_S))
            : DEFAULT_ASSOCIATION_TIMEOUT_S},
      association_request_ttl_s_ {
            app_opt_.connection_test_parameters.includes(conn_par::ASSOCIATION_REQUEST_TTL_S)
            ? static_cast<unsigned int>(
                app_opt_.connection_test_parameters.get<int>(conn_par::ASSOCIATION_REQUEST_TTL_S))
            : DEFAULT_ASSOCIATION_REQUEST_TTL_S},
      persist_connections_ {
            app_opt_.connection_test_parameters.includes(conn_par::PERSIST_CONNECTIONS)
            ? app_opt_.connection_test_parameters.get<bool>(conn_par::PERSIST_CONNECTIONS)
            : false},
      show_stats_ {
            app_opt_.connection_test_parameters.includes(conn_par::SHOW_STATS)
            ? app_opt_.connection_test_parameters.get<bool>(conn_par::SHOW_STATS)
            : false},
      current_run_ {app_opt_},
      results_file_name_ {(boost::format("connection_test_%1%.csv")
                           % util::get_short_datetime()).str()},
      results_file_stream_ {(fs::path(app_opt_.results_dir)
                             / results_file_name_).string()},
      keepalive_thread_ {},
      keepalive_mtx_ {},
      keepalive_cv_ {},
      stop_keepalive_task_ {false}
{
    if (!results_file_stream_.is_open())
        throw fatal_error {((boost::format("failed to open %1%")
                             % results_file_name_).str())};
}

void connection_test::start()
{
    auto start_time = std::chrono::system_clock::now();
    LOG_INFO("Requested %1% runs", num_runs_);
    boost::format run_msg_fmt {"Starting %1%"};
    display_setup();

    do {
        boost::nowide::cout << (run_msg_fmt % current_run_.to_string()).str()
                            << std::endl;
        auto results = perform_current_run();
        results_file_stream_ << results;
        boost::nowide::cout << results;

        if (show_stats_) {
            results_file_stream_ << ",";
            results_file_stream_ << results.conn_stats;
            boost::nowide::cout << results.conn_stats;
        }

        results_file_stream_ << '\n';
        boost::nowide::cout << '\n';
        ++current_run_;

        if (current_run_.idx <= num_runs_) {
            // Be nice with the broker and pause (2000 + pause * num endpoints)
            std::this_thread::sleep_for(std::chrono::milliseconds(
                2000 + inter_run_pause_ms_
                       * current_run_.num_endpoints
                       * current_run_.concurrency));
        }
    } while (current_run_.idx <= num_runs_);

    display_execution_time(start_time);
}

void connection_test::display_setup()
{
    const auto& p = app_opt_.connection_test_parameters;
    boost::nowide::cout
        << "\nConnection test setup:\n"
        << "  " << p.get<int>(conn_par::CONCURRENCY) << " concurrent sets (+"
        << p.get<int>(conn_par::CONCURRENCY_INCREMENT) << " per run) of "
        << p.get<int>(conn_par::NUM_ENDPOINTS) << " endpoints (+"
        << p.get<int>(conn_par::ENDPOINTS_INCREMENT) << " per run)\n"
        << "  " << num_runs_ << " runs, (2000 + "
        << inter_run_pause_ms_ << " * num_endpoints) ms pause between each run\n"
        << "  " << inter_endpoint_pause_ms_
        << " ms pause between each set connection";

    if (randomize_pause_)
        boost::nowide::cout << " (mean value - exp. distribution)";

    boost::nowide::cout
        << "\n  WebSocket connection timeout " << ws_connection_timeout_ms_ << " ms\n"
        << "  Association timeout " << association_timeout_s_
        << " s; Association Request TTL " << association_request_ttl_s_ << " s\n"
        << "  keep WebSocket connections alive: ";

    if (persist_connections_) {
        boost::nowide::cout << "yes, by pinging every "
                            << ws_connection_check_interval_s_ << " s\n\n";
    } else {
        boost::nowide::cout << "no\n\n";
    }
}

void connection_test::display_execution_time(
        std::chrono::system_clock::time_point start_time)
{
    auto end_time = std::chrono::system_clock::now();
    auto duration_m = std::chrono::duration_cast<std::chrono::minutes>(
            end_time - start_time).count();
    auto duration_s = std::chrono::duration_cast<std::chrono::seconds>(
            end_time - start_time).count() - (duration_m * 60);

    boost::nowide::cout
        << "\nConnection test: finished in " << duration_m << " m "
        << duration_s << " s";

    if (current_run_.idx <= num_runs_) {
        auto executed_runs = current_run_.idx - 1;
        boost::nowide::cout
            << "; only the first "
            << (executed_runs > 1
                    ? ((boost::format("%1% runs were") % executed_runs)).str()
                    : "run was")
            << " executed\n" << std::endl;
    } else {
        boost::nowide::cout  << "\n" << std::endl;
    }
}

// Connection Task

int connect_clients_serially(std::vector<std::shared_ptr<client>> client_ptrs,
                             std::vector<uint32_t> pauses_ms,
                             bool randomize,
                             std::shared_ptr<connection_timings_accumulator> timings_acc_ptr,
                             const unsigned int task_id)
{
    assert(pauses_ms.size() > 0);

    if (pauses_ms.size() > 1)
        assert(pauses_ms.size() == client_ptrs.size());

    int num_failures {0};
    auto start = std::chrono::system_clock::now();
    bool associated;
    int idx {0};

    // Initialize and use the constant pause value, if we're not randomizing
    std::chrono::milliseconds pause_ms {pauses_ms[0]};

    for (auto &e_p : client_ptrs) {
        if (randomize)
            pause_ms = std::chrono::milliseconds(pauses_ms[idx++]);

        try {
            e_p->connect(1);
            associated = e_p->isAssociated();

            if (timings_acc_ptr) {
                auto ws_timings = e_p->getConnectionTimings();
                timings_acc_ptr->accumulate_tcp_us(
                        ws_timings.getTCPInterval().count());
                timings_acc_ptr->accumulate_ws_open_handshake_us(
                        ws_timings.getOpeningHandshakeInterval().count());

                if (associated) {
                    auto ass_timings = e_p->getAssociationTimings();
                    timings_acc_ptr->accumulate_association_ms(
                            ass_timings.getAssociationInterval().count());
                }
            }

            std::this_thread::sleep_for(pause_ms);

            // Must still be associated after the pause for success
            associated &= e_p->isAssociated();

            if (!associated) {
                LOG_WARNING("Connection Task %1%: client %2% is not associated "
                            "after %3% ms",
                            task_id, e_p->configuration.common_name, pause_ms.count());
                num_failures++;
            }
        } catch (const PCPClient::connection_error& e) {
            LOG_WARNING("Connection Task %1%: client %2% failed to connect (%3%) "
                        "- will wait %4% ms",
                        task_id, e_p->configuration.common_name, e.what(),
                        pause_ms.count());
            num_failures++;
            std::this_thread::sleep_for(pause_ms);
        } catch (const std::exception& e) {
            LOG_WARNING("Connection Task %1%: unexpected error for client %2% "
                        "(%3%) - will wait %4% ms",
                        task_id, e_p->configuration.common_name, e.what(),
                        pause_ms.count());
            num_failures++;
            std::this_thread::sleep_for(pause_ms);
        }
    }

    for (auto &e_p : client_ptrs) {
        if (timings_acc_ptr && e_p->isAssociated()) {
            auto ass_timings = e_p->getAssociationTimings();
            timings_acc_ptr->accumulate_session_duration_ms(
                    ass_timings.getOverallSessionInterval_ms().count());
        }
    }

    auto d = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now() - start).count();
    LOG_INFO("Connection Task %1%: completed in %2%",
             task_id, normalizeTimeInterval(d));
    return num_failures;
}

static const std::string CONNECTION_TEST_CLIENT_TYPE {"agent"};

connection_test_result connection_test::perform_current_run()
{
    connection_test_result results {current_run_};
    std::shared_ptr<connection_timings_accumulator> timings_acc_ptr {nullptr};

    if (show_stats_)
        timings_acc_ptr.reset(new connection_timings_accumulator());

    std::unique_ptr<exponential_integers> rng_ptr {
        randomize_pause_
        ? new exponential_integers(mean_connection_rate_Hz_, current_run_.rng_seed)
        : nullptr};

    uint32_t max_tot_pause_ms {0};
    std::vector<std::vector<std::shared_ptr<client>>> all_clients_ptrs {};
    std::vector<std::future<int>> task_futures {};
    client_configuration c_cfg {"0000agent",
                                CONNECTION_TEST_CLIENT_TYPE,
                                app_opt_.broker_ws_uris,
                                app_opt_.certificates_dir,
                                ws_connection_timeout_ms_,
                                association_timeout_s_,
                                association_request_ttl_s_};

    // Spawn concurrent Connection Tasks

    auto add_client =
        [&c_cfg] (std::string name,
                  std::vector<std::shared_ptr<client>>& task_client_ptrs) -> void
        {
            c_cfg.common_name = name;
            c_cfg.update_cert_paths();
            auto c_ptr = std::make_shared<client>(c_cfg);
            task_client_ptrs.push_back(c_ptr);
        };

    auto agents_it       = app_opt_.agents.begin();
    auto agents_end      = app_opt_.agents.end();
    auto controllers_it  = app_opt_.controllers.begin();
    auto controllers_end = app_opt_.controllers.end();

    auto get_name =
        [&agents_it, &agents_end, &controllers_it, &controllers_end] () -> std::string
        {
            std::string name {};
            if (agents_it != agents_end) {
                name = *agents_it;
                agents_it++;
            } else if (controllers_it != controllers_end) {
                name = *controllers_it;
                controllers_it++;
            } else {
                assert(false);
            }

            return name;
        };

    for (auto task_idx = 0; task_idx < current_run_.concurrency; task_idx++) {
        std::vector<std::shared_ptr<client>> task_client_ptrs {};
        std::vector<uint32_t> pauses_ms {};
        uint32_t tot_pause_ms {0};

        for (auto idx = 0; idx < current_run_.num_endpoints; idx++) {
            add_client(get_name(), task_client_ptrs);

            if (randomize_pause_) {
                auto p = (*rng_ptr)();
                tot_pause_ms += p;
                pauses_ms.push_back(std::move(p));
            }
        }

        if (randomize_pause_) {
            if (max_tot_pause_ms < tot_pause_ms)
                max_tot_pause_ms = tot_pause_ms;
        } else {
            if (tot_pause_ms == 0) {
                // Push just one value in case of constant pause;
                // the Connection Task will figure it out
                pauses_ms.push_back(inter_endpoint_pause_ms_);
                tot_pause_ms = current_run_.num_endpoints * inter_endpoint_pause_ms_;
                max_tot_pause_ms = tot_pause_ms;
            }
        }

        // Copy client pointers so this thread, or the Keep Alive one,
        // will be in charge of destroying them, otherwise we would
        // include the close handshake times when reporting the
        // overall time to connect
        all_clients_ptrs.emplace_back(task_client_ptrs);

        try {
            task_futures.push_back(
                std::async(std::launch::async,
                           &connect_clients_serially,
                           std::move(task_client_ptrs),
                           std::move(pauses_ms),
                           randomize_pause_,
                           timings_acc_ptr,
                           task_idx));
            LOG_DEBUG("Run #%1% - started Connection Task %2%",
                      current_run_.idx, task_idx + 1);
        } catch (std::exception& e) {
            boost::nowide::cout
                << "\n" << util::red("   [ERROR]   ")
                << "failed to start Connection Task - thread error: "
                << e.what() << "\n";
            throw fatal_error { "failed to start Connection Task threads" };
        }
    }

    // Display timeout (the total pause may have ben randomized)

    auto timeout_ms = max_tot_pause_ms + current_run_.total_endpoint_timeout_ms;
    std::chrono::seconds timeout_s {timeout_ms / 1000};

    boost::nowide::cout << "                timeout for establishing all connections "
                        << normalizeTimeInterval(timeout_ms)
                        << std::endl;

    // Start the Keep Alive Task

    if (persist_connections_) {
        stop_keepalive_task_ = false;

        try {
            keepalive_thread_ = std::thread {&connection_test::keepalive_task,
                                             this,
                                             std::move(all_clients_ptrs)};
        } catch (std::exception& e) {
            boost::nowide::cout
            << "\n" << util::red("   [ERROR]   ")
            << "failed to start Keep Alive Task - thread error: "
            << e.what() << "\n";
            throw fatal_error { "failed to start Keep Alive Task thread" };
        }
    }

    // Wait for threads to complete and get the number of failures (as futures)

    auto start = std::chrono::system_clock::now();

    for (std::size_t thread_idx = 0; thread_idx < task_futures.size(); thread_idx++) {
        auto elapsed = std::chrono::system_clock::now() - start;
        auto timeout = timeout_s > elapsed
                       ? timeout_s - elapsed
                       : std::chrono::seconds::zero();

        if (task_futures[thread_idx].wait_for(timeout) != std::future_status::ready) {
            LOG_WARNING("Run #%1% - Connection Task %2% timed out",
                        current_run_.idx, thread_idx);
            results.num_failures += current_run_.num_endpoints;
        } else {
            try {
                results.num_failures += task_futures[thread_idx].get();
            } catch (std::exception& e) {
                LOG_WARNING("Run #%1% - Connection Task %2% failure: %3%",
                            current_run_.idx, thread_idx, e.what());
                results.num_failures += current_run_.num_endpoints;
            }
        }
    }

    // Report completion and get timing stats

    boost::nowide::cout << "                done - "
                           "closing connections and retrieving results"
                        << std::endl;
    results.set_completion();

    if (show_stats_)
        results.conn_stats = timings_acc_ptr->get_connection_stats();

    LOG_INFO("Run #%1% - got Connection Task results; about to close connections",
             current_run_.idx);

    boost::nowide::cout << "Press return to continue..." << std::flush;
    boost::nowide::cin.get();

    // Close connections

    if (persist_connections_) {
        // Stop the Keepalive Task
        assert(all_clients_ptrs.empty());
        stop_keepalive_task_ = true;
        keepalive_cv_.notify_one();

        if (keepalive_thread_.joinable()) {
            keepalive_thread_.join();
            LOG_INFO("Run #%1% - Keep Alive Task completed", current_run_.idx);
        } else {
            LOG_ERROR("The Keep Alive Task thread is not joinable");
        }
    } else {
        if (current_run_.num_endpoints > 0)
            assert(!all_clients_ptrs.empty());

        close_connections_concurrently(std::move(all_clients_ptrs));
    }

    return results;
}

static constexpr uint32_t PING_PAUSE_MS {2};

void connection_test::keepalive_task(
        std::vector<std::vector<std::shared_ptr<client>>> all_clients_ptrs)
{
    assert(persist_connections_);
    std::chrono::system_clock::time_point now {};
    static const std::chrono::milliseconds ping_pause_ms {PING_PAUSE_MS};
    uint32_t ping_loop_duration_s {
        (current_run_.num_endpoints * current_run_.concurrency * PING_PAUSE_MS) / 1000};
    std::chrono::seconds check_interval {
        ws_connection_check_interval_s_ > ping_loop_duration_s
        ? ws_connection_check_interval_s_ - ping_loop_duration_s
        : 1};
    std::unique_lock<std::mutex> lck {keepalive_mtx_};
    LOG_INFO("Run #%1% - starting Keep Alive Task, period equal to %2% s",
             current_run_.idx, check_interval.count());

    while (!stop_keepalive_task_) {
        now = std::chrono::system_clock::now();
        keepalive_cv_.wait_until(lck, now + check_interval);

        for (auto& task_clients_ptrs : all_clients_ptrs) {
            for (auto &c_ptr : task_clients_ptrs) {
                if (stop_keepalive_task_)
                    break;

                if (c_ptr->isAssociated()) {
                    try {
                        c_ptr->ping();
                    } catch (const std::exception &e) {
                        LOG_ERROR("Client %1% failed to ping (%2%)",
                                  c_ptr->configuration.common_name, e.what());
                    }

                    std::this_thread::sleep_for(ping_pause_ms);
                }
            }
        }
    }

    close_connections_concurrently(std::move(all_clients_ptrs));
}

void connection_test::close_connections_concurrently(
        std::vector<std::vector<std::shared_ptr<client>>> all_clients_ptrs)
{
    LOG_INFO("About to close all connections");

    if (all_clients_ptrs.size() > 1) {
        // Trigger client dtors concurrently
        std::vector<std::thread> dtor_threads{};

        try {
            for (auto &t_c_ptrs : all_clients_ptrs) {
                dtor_threads.push_back(std::thread {
                        [&t_c_ptrs]() {
                            try {
                                for (auto &c : t_c_ptrs)
                                    c.reset();
                            } catch (const std::exception &e) {
                                LOG_ERROR("Exception closing connection: %1%",
                                          e.what());
                            }
                        }});
            }
        } catch (const std::exception &e) {
            LOG_ERROR("Failed to destroy clients (%1%)", e.what());
        }

        for (auto &t : dtor_threads) {
            try {
                if (t.joinable())
                    t.join();
            } catch (const std::exception &e) {
                LOG_ERROR("Exception joining threads: %1%", e.what());
            }
        }
    }
}

}  // namespace pcp_test
