#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <deque>

std::vector<char> read_file(char const *pathname)
{
    std::ifstream file(pathname, std::ios::binary);
    std::vector<char> result;
    if (file.is_open()) {
        file.seekg(0, std::ios::end);
        uint64_t size = file.tellg();
        file.seekg(0, std::ios::beg);
        result.resize(size);
        file.read(result.data(), result.size());
    }
    return result;
}

// Producer consumer queue that is designed to
// handle multiple producers and multiple consumers.
// What makes it handle it well is the fact that it
// doesn't try to be clever about notifying conditions,
// if one tries to be smart about skipping wakeups in this
// scenario, one is very likely to mess it up and 
// miss a wakeup
template<typename T>
class producer_consumer_q {
public:
    // Default constructor leaves the capacity at 
    // the default value (assigned below)
    producer_consumer_q() = default;

    // You can specify a capacity
    explicit producer_consumer_q(size_t capacity)
        : capacity(capacity)
    {
    }

    // Tell the queue that it should not expect to receive
    // any more items, so if anyone is waiting, it should wake
    // them up. They will peek at add_done if the queue is empty
    // and they will stop waiting and return to the caller that
    // asked to take from the queue
    void add_done()
    {
        scoped_lock lock(queue_lock);
        added_done = true;
        not_empty.notify_all();
    }

    // This overload copies the input to be copied omtp the qieie
    void add(T const& item)
    {
        scoped_lock lock(queue_lock);
        wait_for_space(lock);
        queue.push_back(item);
        not_empty.notify_all();
    }

    // Move constructor that takes rvalue reverence for move semantics
    // This causes the item to be "moved" into the queue
    void add(T&& item)
    {
        scoped_lock lock(queue_lock);
        wait_for_space(lock);
        queue.push_back(std::move(item));
        not_empty.notify_all();
    }

    // Template gets the number of items in the array passed in as N
    template<size_t N>
    bool take(T (&items)[N])
    {
        scoped_lock lock(queue_lock);

        // If the wait function told us there are no more
        // then return false
        if (!wait_for_N(lock, N))
            return false;

        // Move N items from the head of the queue to items[]
        for (size_t i = 0; i < N; ++i) {
            // Move the item from the front of the queue 
            // into the result array
            items[i] = std::move(queue.front());

            // Get rid of the item we consumed from the deque
            queue.pop_front();
        }
        // Tell code that might be waiting to add that they
        // should check the size again
        not_full.notify_all();
        return true;
    }

    // Return the number of items in the queue
    size_t size() const
    {
        scoped_lock lock(queue_lock);
        return queue.size();
    }

    // Returns true if the queue is empty
    bool empty() const
    {
        scoped_lock lock(queue_lock);
        return queue.empty();
    }

private:
    using scoped_lock = std::unique_lock<std::mutex>;

    // The capacity member sets the maximum number of items that
    // may be in the queue. If you try to add an item when the queue
    // is full, this function will wait for the size of the queue
    // to fall below the maximum
    void wait_for_space(scoped_lock &lock)
    {
        while (queue.size() >= capacity)
            not_full.wait(lock);
    }

    // This efficiently waits until there are N items in the queue,
    // and returns true,
    // If added_done becomes true when there are not enough 
    // items, it returns false
    // Returning true means (the required number of itmes are in the queue)
    // Returning false means (stop waiting, they are never coming)
    bool wait_for_N(scoped_lock &lock, size_t N)
    {
        // Keep waiting until there are N items, or added_done is true
        // (not_empty is notified by the add() functions when they put
        // something into the queue)
        while (queue.size() < N && !added_done)
            not_empty.wait(lock);
        
        // It only makes it here if add_done was true and queue is empty
        // so return false if there is no point waiting, add is done
        return queue.size() >= N;
    }

    // This mutex synchronizes the access to the queue and the added_done variable
    std::mutex queue_lock;
    
    // This condition variable is notified by add methods when items are added
    std::condition_variable not_empty;

    // This condition variable is notified by take methods when items are removed
    std::condition_variable not_full;

    // This is a container for which it is very efficient to add and remove items
    // from both the front and back. Perfect for a queue. As in "deck" of cards.
    std::deque<T> queue;
    size_t capacity = SIZE_MAX;
    bool added_done = false;
};

// Loop over all of the threads in the vector and join them
// You MUST .join() every thread you create, or detach() it
void wait_for_workers(std::vector<std::thread> &workers)
{    
    for (std::thread &worker : workers)
        worker.join();
}

int process_file(char const *pathname)
{
    // Read entire file into one buffer

    std::vector<char> file;
    try {
        file = read_file(pathname);
    } catch (...) {
        std::cerr << "Error reading file\n";
        return EXIT_FAILURE;
    }

    // Set up a pointer to the beginning and end of the file
    char const *st = file.data();
    char const *en = st + file.size();

    // Create a producer consumer queue for mapping the lines 
    // (fake work just sleeps and returns n)
    producer_consumer_q<int> map_queue;

    // Create a producer consumer queue for reducing pairs of numbers
    // to their sum. The result of the sum is pushed back into the
    // sum queue, to be picked up by this worker, or even another one.
    // This causes it to magically reduce the sums in parallel.
    producer_consumer_q<int> sum_queue;

    // Ask the standard library how many processors this machine has
    // (The number of threads that can actually run at the same time)
    size_t cpu_count = std::thread::hardware_concurrency();

    size_t map_worker_count = cpu_count;
    size_t sum_worker_count = cpu_count;

    char const *env_override;

    env_override = getenv("MAP_WORKERS");
    if (env_override) {
        map_worker_count = atoi(env_override);
        std::cerr << "Using " << map_worker_count << " map workers\n";
    }
    env_override = getenv("SUM_WORKERS");
    if (env_override) {
        sum_worker_count = atoi(env_override);
        std::cerr << "Using " << sum_worker_count << " sum workers\n";
    }
    auto map_item = [](int n)
    {
        std::this_thread::sleep_for(std::chrono::seconds(10));
        return n;
    };



    // This is the code executed in the sum threads
    auto sum_worker = [&]()
    {
        // Keep taking 2 items, add them, 
        // and put the sum back into the queue
        int items[2];
        while (sum_queue.take(items)) {
            int sum = items[0] + items[1];
            sum_queue.add(sum);
        }

        // We don't sum_queue.add_done() yet,
        // we do that in the main thread
        // after we have waited for all of
        // the sum_worker threads to exit
        // (if we did it here, other workers
        // might have more to add and saying
        // it's "done" would not be correct (yet))
    };

    // This is the code executed in the map threads
    auto map_worker = [&]()
    {
        // Keep reading 1 item at a time and map it through map_item
        int items[1];
        while (map_queue.take(items)) {
            // take the item from the queue and pass it to the "map"
            // part, and replace the item with its return value
            items[0] = map_item(items[0]);

            // And add the return value to the sum queue to be summed
            sum_queue.add(items[0]);
        }

        // Same thing here, don't sum_queue.add_done() until
        // we are sure it is late enough, later
    };

    // These are the worker threads
    std::vector<std::thread> map_workers;
    std::vector<std::thread> sum_workers;
    // Reserve to avoid it having to resize the vector
    map_workers.reserve(map_worker_count);
    sum_workers.reserve(sum_worker_count);

    // Add one worker of each type for each cpu
    // The map workers eventually drain out
    // so it ends up with just sum workers, one
    // per cpu. At first, the map workers do most
    // of the work, then later, sum workers do all of
    // the work
    for (size_t n = 0; n < map_worker_count; ++n) 
        // This starts the thread
        map_workers.emplace_back(map_worker);

    for (size_t n = 0; n < sum_worker_count; ++n)
        sum_workers.emplace_back(sum_worker);

    // While we haven't consumed every line in the file
    while (st < en) {
        // Scan forward for the first \n, 
        // and set up line_len to be its offset from st
        // but also make sure we don't fall off the end
        size_t line_len = 0;
        while (st + line_len < en && st[line_len] != '\n')
            ++line_len;

        // Convert the digits to a number (like atoi, but this
        // code doesn't require the null terminator)
        int n = 0;
        for (size_t i = 0; i < line_len; ++i) {
            if (st[i] < '0' || st[i] > '9')
                break;
            
            // Base 10 (lowest digit becomes 0)
            n *= 10;
            // Get the ASCII value of the character 
            // and subtract the ASCII value of '0'
            // to get what digit it is as a number
            // Add that digit to the total
            // Modifies lowest digit
            n += st[i] - '0';
        }

        // Move the pointer to the newline at the end of the line
        st += line_len;

        // Count how many newlines are at the end of the line
        // and make sure we don't fall off the end
        size_t newline_count = 0;
        while (st < en && st[0] == '\n')
            ++st;

        // Add the number to the mapping queue
        // to be processed by map_worker
        map_queue.add(n);
    }

    // Tell the map queue that it should not wait if it
    // finds insufficient items in the queue
    map_queue.add_done();

    // Wait for all of the map worker threads to exit
    wait_for_workers(map_workers);

    // Tell the sum queue we don't expect any new data from
    // the map threads, we are sure because we just waited
    // for all of the map threads to exit
    sum_queue.add_done();

    // Wait for the sum workers to exit
    wait_for_workers(sum_workers);

    // At the end, there will be a single value in the sum queue
    // Get it, it's the final sum
    int result[1];
    sum_queue.take(result);

    std::cout << "The sum is " << result[0] << "\n";

    // Exit with 0 exitcode
    return EXIT_SUCCESS;
}

int main(int argc, char **argv)
{
    // Requires a command line parameter, the pathname to read
    if (argc <= 1) {
        std::cerr << "Pass input filename on command line\n";
        return EXIT_FAILURE;
    }

    return process_file(argv[1]);
}
