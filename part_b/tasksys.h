#ifndef _TASKSYS_H
#define _TASKSYS_H

#include <condition_variable>
#include <mutex>
#include <thread>
#include <numeric>
#include <atomic>
#include <iostream>
#include <vector>

// testing this now
#include <deque>
#include <map>

#include <stdio.h>

#include "itasksys.h"

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial : public ITaskSystem
{
public:
    TaskSystemSerial(int num_threads);
    ~TaskSystemSerial();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn : public ITaskSystem
{
public:
    TaskSystemParallelSpawn(int num_threads);
    ~TaskSystemParallelSpawn();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning : public ITaskSystem
{
public:
    TaskSystemParallelThreadPoolSpinning(int num_threads);
    ~TaskSystemParallelThreadPoolSpinning();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();
};

/*
Task group
- taskid
- runnable
- dep_list
- complete  -> false
- launched  -> false
- num_total_tasks
- current_task_index -> 0 (needs syncrhonization with a mutex or atomic)
*/
class TaskGroup
{

public:
    TaskGroup(
        TaskID task_id,
        IRunnable *runnable,
        int num_total_tasks,
        const std::vector<TaskID> &dep_list) : task_id(task_id),
                                               runnable(runnable),
                                               num_total_tasks(num_total_tasks),
                                               dep_list(dep_list)
    {
        this->current_task_index = 0;
        // print to cout the len of deps
        // std::cout << "TaskGroup: " << task_id << ", " << dep_list.size() << std::endl;
    }

    TaskID task_id;
    IRunnable *runnable;
    int num_total_tasks;
    const std::vector<TaskID> dep_list;

    std::atomic<int> *tasks_complete = new std::atomic<int>(0);
    std::atomic<bool> *complete = new std::atomic<bool>(false);
    bool launched = false;

    // needs to be synchronized
    std::atomic<int> current_task_index;
};

struct TaskUnit
{
    TaskID task;
    int task_index;
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping : public ITaskSystem
{
public:
    TaskSystemParallelThreadPoolSleeping(int num_threads);
    ~TaskSystemParallelThreadPoolSleeping();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();

private:
    // work ready is for worker threads to wake up
    std::condition_variable *work_ready = new std::condition_variable();
    // work done is for sync to wake up
    std::condition_variable *work_done = new std::condition_variable();

    std::thread *workers;

    std::mutex *work_queue_mutex = new std::mutex;
    std::mutex *sync_mutex = new std::mutex;

    /*
    Task System
    - Shutdown workers (needs sync)
    - Task groups
    - Task queue (needs sync)
    */

    int thread_total_num;
    int all_task_groups_done = false;
    int task_group_incrementer = 0;

    bool running = false;

    std::map<TaskID, TaskGroup *> task_groups = std::map<TaskID, TaskGroup *>();
    std::deque<TaskUnit> tasks_pending = std::deque<TaskUnit>();

    void dynamicSleepingWorker(int thread_id);
};

#endif
