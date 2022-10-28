#include "tasksys.h"

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char *TaskSystemSerial::name()
{
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads) : ITaskSystem(num_threads)
{
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable *runnable, int num_total_tasks)
{
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                          const std::vector<TaskID> &deps)
{
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync()
{
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelSpawn::name()
{
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads) : ITaskSystem(num_threads)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                 const std::vector<TaskID> &deps)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync()
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSpinning::name()
{
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads) : ITaskSystem(num_threads)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable, int num_total_tasks)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync()
{
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSleeping::name()
{
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads) : ITaskSystem(num_threads)
{
    thread_total_num = num_threads;
    workers = new std::thread[thread_total_num];

    for (int i = 0; i < thread_total_num; i++)
    {
        workers[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::dynamicSleepingWorker, this, i);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping()
{
    {
        std::lock_guard<std::mutex> set_variables_lock(*work_queue_mutex);
        all_task_groups_done = true;
    }
    // work_done->notify_one();
    // work_ready->notify_all();
    for (int i = 0; i < thread_total_num; i++)
    {
        workers[i].join();
    }
    // delete work_queue_mutex;
    // delete[] workers;
    // delete work_ready;
    // delete work_done;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable, int num_total_tasks)
{
    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();
    return;
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    // This is the main thread so you don't need locks here yet.
    auto task_id = task_group_incrementer;

    task_groups[task_id] = new TaskGroup(
        task_id,
        runnable,
        num_total_tasks,
        deps);

    task_group_incrementer += 1;

    return task_id;
}

/*

Main thread
    lock work queue mutex
        queue work into work queue
    release work queue mutex lock

    wait for worker to notify us

Worker
    Wait for work ready
    Wait for work queue mutex to be ready

workers will figure out which task group to work on
workers will then gfigure which task in task group to complete
before running the runnable
workers will unlock

*/

void TaskSystemParallelThreadPoolSleeping::sync()
{
    // Lock the work queue while scan
    // This is okay because we won't lock again until a worker has notified us
    // The waiting reduces lock contention
    for (;;)
    {
        std::unique_lock<std::mutex> lock(*work_queue_mutex);
        bool all_task_groups_complete = true;

        int tasks_complete = 0;
        int tasks_incompelte = 0;

        // https: // stackoverflow.com/questions/26281979/c-loop-through-map
        for (const auto &[task_id, task] : task_groups)
        {
            if (task->complete)
            {
                tasks_complete += 1;
                continue;
            }
            tasks_incompelte += 1;

            all_task_groups_complete = false;

            // loop through dependencies to see if they are done
            bool all_deps_ready = true;
            // std::cout << "Checking dep list. " << task->dep_list.size() << std::endl;
            for (const auto dep_id : task->dep_list)
            {

                if (task_groups[dep_id]->complete)
                {
                    continue;
                }

                all_deps_ready = false;
            }

            // if all deps ready and not launched, push it into the work queue and mark it launched
            if (all_deps_ready && !(task->launched))
            {
                std::cout << "Launching " << task->task_id << " with deps:  " << task->dep_list.size() << std::endl;
                task->launched = true;
                task_groups_to_complete.push_back(task);
            }
        }

        bool is_queue_empty = task_groups_to_complete.empty();
        work_queue_mutex->unlock();

        if (all_task_groups_complete)
        {
            std::cout << "returning because all tasks groups completed: " << tasks_complete << "," << tasks_incompelte << std::endl;
            return;
        }

        // if the queue is empty
        if (is_queue_empty)
        {

            std::cout << "Sleeping main thread until work is ready. Tasks complete " << tasks_complete << "," << tasks_incompelte << std::endl;
            std::this_thread::sleep_for(std::chrono::nanoseconds(10));
            continue;
        }

        // if (all_task_groups_done)
        // {
        //     std::cout << "returning because all tasks groups done" << std::endl;
        //     return;
        // }
    }
}

void TaskSystemParallelThreadPoolSleeping::dynamicSleepingWorker(int thread_id)
{
    int local_counter = -1;
    // Lock the task queue until you hear from the main thread that there is more work to do
    for (;;)
    {
        std::unique_lock<std::mutex> work_lock(*work_queue_mutex);

        if (all_task_groups_done)
        {
            work_lock.unlock();
            return;
        }

        // *only* wait if there's no work in the queue
        // otherwise, just work on the next task
        if (task_groups_to_complete.empty())
        {
            // sleep the thread
            work_lock.unlock();
            std::this_thread::sleep_for(std::chrono::nanoseconds(10));
            continue;
        }

        TaskGroup *task_to_do = task_groups_to_complete.front();

        // Save the current task
        int id = task_to_do->task_id;
        int local_ctr = task_to_do->current_task_index.load();
        bool is_last = local_ctr == (task_to_do->num_total_tasks - 1);
        int num_total_tasks = task_to_do->num_total_tasks;

        task_to_do->current_task_index += 1;

        // Check if this task is the finale
        if (is_last)
        {
            // if it is, pop it
            std::cout << "Popping task: " << id << ", thread: " << thread_id << std::endl;
            if (task_groups_to_complete.empty())
            {
                continue;
            }
            else
            {
                task_groups_to_complete.pop_front();
            }
        }

        // Relieve the lock
        work_lock.unlock();

        // Run the task
        task_to_do->runnable->runTask(local_ctr, num_total_tasks);

        if (is_last)
        {
            // print to cout
            std::cout << "Finished task  " << id << std::endl;
            // std::cout << "Thread " << thread_id << " finished task group for task " << id << std::endl;

            work_lock.lock();
            task_to_do->complete = true;

            std::cout << "unlock" << std::endl;
            work_lock.unlock();
        }

        // std::cout << "Finished task  " << id << std::endl;
        // lock.unlock();
        // work_done->notify_one();
    }
}
