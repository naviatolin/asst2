#ifndef _TASKSYS_H
#define _TASKSYS_H

#include <condition_variable>
#include <mutex>
#include <thread>
#include <numeric>
#include <atomic>
#include <iostream>
#include <vector>

#include <stdio.h>

#include "itasksys.h"

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
    private:
        int thread_total_num;
        std::thread* workers;
        std::mutex* mutex;
        void dynamicTaskAssignment(IRunnable* runnable, int num_total_tasks, int *counter);
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */

class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
    private:
        int thread_total_num;
        int _num_total_tasks_ = -1;
        IRunnable* _runnable_ = nullptr;
        std::thread* workers;
        std::mutex* mutex = new std::mutex;
        bool join_threads = false;
        int counter = 0;

        int done_threads = 0;
        bool* thread_status;
        void dynamicSpinningWorker(int thread_id);
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */


class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();

    private:
        int thread_total_num;
        int _num_total_tasks_ = -1;
        IRunnable* _runnable_ = nullptr;
        std::thread* workers;
        std::mutex* mutex = new std::mutex;
        bool join_threads = false;
        int counter = 0;

        int done_threads = 0;
        bool* thread_status;

        std::condition_variable* worker_condition = new std::condition_variable();
        std::condition_variable* run_condition = new std::condition_variable();
        void dynamicSleepingWorker(int thread_id);
};

#endif
