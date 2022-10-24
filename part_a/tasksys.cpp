#include "tasksys.h"
#include <thread>


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    n_threads = num_threads;
    workers = new std::thread[n_threads];
    key = new std::mutex;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::block(IRunnable* runnable, int block_num, int num_total_tasks) {
    int mintask = n_threads * block_num;
    int maxtask = mintask + n_threads;
    for(int i =mintask; i < maxtask; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

void TaskSystemParallelSpawn::interleaved(IRunnable* runnable, int thread_num, int num_total_tasks) {
    int mintask = thread_num;
    for(int i = mintask; i < num_total_tasks; i+=n_threads) {
        runnable->runTask(i, num_total_tasks);
    }
}

void TaskSystemParallelSpawn::dynamic(IRunnable* runnable, int* counter, int num_total_tasks) {
    int local_ctr = -1 ;
    while(local_ctr < num_total_tasks) {
        key->lock();
        local_ctr = *counter;
        *counter += 1;
        key->unlock();
        if (local_ctr >= num_total_tasks) break;
        runnable->runTask(local_ctr, num_total_tasks);
    }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //    
    int counter = 0;
    for (int i = 0; i < n_threads; i++) {
        workers[i] = std::thread(&TaskSystemParallelSpawn::dynamic, this, runnable, &counter, num_total_tasks);
    }
    for (int i = 0; i < n_threads; i++) {
        workers[i].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    n_threads = num_threads;
    key = new std::mutex();
    mutex_condition = new std::condition_variable();
    workers = new std::thread[n_threads];
    counter = new int(0);
    job = nullptr;
    kill = false;
    new_run = false;
    for (int i = 0; i < n_threads; i++) {
        workers[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::work, this);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    key->lock();
    kill = true;
    key->unlock();
    mutex_condition->notify_all();
    for (int i = 0; i < n_threads; i++) {
        workers[i].join();
    }
    delete[] workers;    
    delete key;
    delete mutex_condition;
}

void TaskSystemParallelThreadPoolSpinning::work() {
    int local_ctr = -1;
    IRunnable* local_job = nullptr;
    int total = -1;
    while(1) {
        {   
            std::unique_lock<std::mutex> lock(*key);
            mutex_condition->wait(lock, [this]{return new_run || kill;});
            if (kill) return;
            local_ctr = *counter;
            *counter += 1;
            local_job = job;
            total = _num_total_tasks;
            if (local_ctr >= total) new_run = false;
        }
        if (local_ctr < total) local_job->runTask(local_ctr, total);
    }
}


void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    key->lock();
    job = runnable;
    new_run = true;
    _num_total_tasks = num_total_tasks;
    counter = new int(0);
    key->unlock();
    mutex_condition->notify_all();
    while(1) {
        key->lock();
        if (*counter >= num_total_tasks) {
            key->unlock();
            return;
        }
        key->unlock();
    }
}


TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
