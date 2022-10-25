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

void TaskSystemParallelThreadPoolSpinning::dynamicSpinningWorker(int thread_id) {
    int local_counter = -1;
    for (;;) {
      mutex->lock();
      if (counter < _num_total_tasks_ && thread_status[thread_id] == true){
        local_counter = counter;
        counter += 1;
        mutex->unlock();
        _runnable_->runTask(local_counter, _num_total_tasks_);
      }
      else if (thread_status[thread_id] == false && counter < _num_total_tasks_) {
        done_threads--;
        mutex->unlock();
        thread_status[thread_id] = true;
        continue;
      }
      else if (thread_status[thread_id] == true && counter >= _num_total_tasks_) {
        done_threads++;
        mutex->unlock();
        thread_status[thread_id] = false;
      }
      else if (join_threads) {
        mutex->unlock();
        return;
      } 
      else {
        mutex->unlock();
      }
    }
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    thread_total_num = num_threads;
    workers = new std::thread[thread_total_num];
    thread_status = new bool[thread_total_num];

    for (int i = 0; i < thread_total_num; i++) {
      thread_status[i] = true;
      workers[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::dynamicSpinningWorker, this, i);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    // reason why we are doing this in destructor is that we are making 400 calls to run
    mutex->lock();
    join_threads = true;
    mutex->unlock();

    for (int i = 0; i < thread_total_num; i++) {
        workers[i].join();
    }

    delete mutex;
    delete[] workers;
    delete[] thread_status;
}


void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    mutex->lock();
    // restart after setting these variables
    _num_total_tasks_ = num_total_tasks;
    _runnable_ = runnable;
    counter = 0;
    mutex->unlock();

    for (;;) {
        mutex->lock();
        if (counter >= num_total_tasks && done_threads >= thread_total_num) {
            mutex->unlock();
            return;
        }
        mutex->unlock();
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

void TaskSystemParallelThreadPoolSleeping::dynamicSleepingWorker(int thread_id) {
    int local_counter = -1;
    for (;;) {
      std::unique_lock<std::mutex> thread_lock(*mutex);
      worker_condition->wait(thread_lock, [&] {
        return !(counter >= _num_total_tasks_ && thread_status[thread_id] == false) || (join_threads);
      });

      if (counter < _num_total_tasks_ && thread_status[thread_id] == true){
        local_counter = counter;
        counter += 1;
        thread_lock.unlock();
        _runnable_->runTask(local_counter, _num_total_tasks_);
        thread_lock.lock();
      }
      else if (thread_status[thread_id] == false && counter < _num_total_tasks_) {
        done_threads--;
        thread_lock.unlock();
        thread_status[thread_id] = true;
        continue;
      }
      else if (thread_status[thread_id] == true && counter >= _num_total_tasks_) {
        done_threads++;
        thread_lock.unlock();
        thread_status[thread_id] = false;
        run_condition->notify_all();
      }
      else if (join_threads) {
        thread_lock.unlock();
        return;
      } 
      else {
        thread_lock.unlock();
      }
    }
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    thread_total_num = num_threads;
    workers = new std::thread[thread_total_num];
    thread_status = new bool[thread_total_num];
    done_threads = thread_total_num;

    for (int i = 0; i < thread_total_num; i++) {
      thread_status[i] = true;
      workers[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::dynamicSleepingWorker, this, i);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    {
      std::lock_guard<std::mutex> set_variables_lock(*mutex);
      join_threads = true;
    }
    worker_condition->notify_all();
    for (int i = 0; i < thread_total_num; i++) {
        workers[i].join();
    }

    delete mutex;
    delete[] workers;
    delete[] thread_status;
    delete run_condition;
    delete worker_condition;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    {
      std::lock_guard<std::mutex> set_variables_lock(*mutex);
      _num_total_tasks_ = num_total_tasks;
      _runnable_ = runnable;
      counter = 0;
    }

    std::unique_lock<std::mutex> wait_until_done_lock(*mutex);
    worker_condition->notify_all();
    run_condition->wait(wait_until_done_lock, [&] {
      return counter >= _num_total_tasks_ && done_threads >= thread_total_num;
    }); return;
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
