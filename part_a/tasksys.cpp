#include "tasksys.h"


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

// Just only added the thread_total_num variable
// I don't know c++ that well so I am wondering if I can
// just access num_threads without this assignment.
TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads)
    : ITaskSystem(num_threads){
    thread_total_num = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::dynamicTaskAssignment(IRunnable* runnable, int num_total_tasks, int* counter){
    int local_ctr = -1 ;
    while(local_ctr < num_total_tasks) {
        mutex->lock();
        local_ctr = *counter;
        *counter += 1;
        mutex->unlock();
        if (local_ctr >= num_total_tasks) break;
        runnable->runTask(local_ctr, num_total_tasks);
    }
    return;
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // Create threads and a mutex
    workers = new std::thread[thread_total_num];
    mutex = new std::mutex;

    // initialize the counter
    int counter = 0;

    // start the threads
    for (int i = 0; i < thread_total_num; i++) {
        workers[i] = std::thread(&TaskSystemParallelSpawn::dynamicTaskAssignment, this, runnable, 
            num_total_tasks, &counter);
    }

    // wait for the threads to join
    for (int i = 0; i < thread_total_num; i++) {
        workers[i].join();
    }

    // delete the mutex and threads as shown in the tutorial
    delete mutex;
    delete[] workers;
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

void TaskSystemParallelThreadPoolSpinning::dynamicSpinningWorker(IRunnable* runnable, int num_total_tasks, int *counter, 
    bool* worker_state, int thread_id) {
    int local_ctr = -1 ;
    for(;;){
        while(local_ctr < num_total_tasks) {
            mutex->lock();
            local_ctr = *counter;
            *counter += 1;
            mutex->unlock();
            if (local_ctr >= num_total_tasks) break;
            runnable->runTask(local_ctr, num_total_tasks);
        }
        mutex->lock();
        worker_state[thread_id] = true;
        mutex->unlock();
    }
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // Create threads and a mutex
    _num_threads_ = num_threads;
    workers = new std::thread[_num_threads_];
    mutex = new std::mutex;
    worker_state = new bool[_num_threads_];

    _runnable_ = nullptr;
    _num_total_tasks_ = -1;
    std::fill(worker_state[0], worker_state[0] + _num_threads_, false);

    // initialize the counter
    int counter = 0;

    // start the thread pools
    for (int i = 0; i < _num_threads_; i++) {
        workers[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::dynamicSpinningWorker, this, _runnable_, 
            _num_total_tasks_, &counter, i);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    delete mutex;
    delete[] workers;
    delete[] worker_state;
}


void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    mutex->lock();
    _num_total_tasks_ = num_total_tasks;
    _runnable_ = runnable;
    mutex->unlock();

    int all_true;

    for (;;) {
        mutex->lock();
        all_true = std::accumulate(worker_state, worker_state + 8, 0);
        mutex->unlock();

        if (all_true == 8) {
            break;
        }
        else {
            continue;
        }
    }

    for (int i = 0; i < _num_threads_; i++) {
        workers[i].join();
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
