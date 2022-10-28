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
    work_done->notify_one();
    work_ready->notify_all();
    for (int i = 0; i < thread_total_num; i++)
    {
        workers[i].join();
    }
    delete work_queue_mutex;
    delete[] workers;
    delete work_ready;
    delete work_done;
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

    task_group_incrementer += 1;

    task_groups[task_id] = new TaskGroup(
        task_group_incrementer,
        runnable,
        num_total_tasks,
        deps);

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
    int tasks_complete;
    for (;;)
    {
        std::unique_lock<std::mutex> lock(*sync_mutex);
        bool all_task_groups_complete = true;

        tasks_complete = 0;

        // https: // stackoverflow.com/questions/26281979/c-loop-through-map
        for (const auto &x : task_groups)
        {
            // const auto &[task_id, task] : task_groups 
            // TaskID task_id = x.first;
            TaskGroup* task = x.second;

            if (task->complete)
            {
                tasks_complete += 1;
                continue;
            }

            if (tasks_complete == task_groups.size()){
                all_task_groups_complete = true;
                return;
            }

            all_task_groups_complete = false;

            // loop through dependencies to see if they are done
            bool all_deps_ready = true;
            for (const auto dep_id : task->dep_list)
            {

                if (task_groups[dep_id]->complete)
                {
                    continue;
                }
                all_deps_ready = false;
            }

            // if all deps ready and not launched, push it into the work queue and mark it launched
            if (all_deps_ready && !task->launched)
            {
                task->launched = true;
                task_groups_to_complete.push_back(task);
                work_ready->notify_all();
            }
        }

        if (!task_groups_to_complete.empty() || !all_task_groups_complete)
        {
            std::this_thread::sleep_for(std::chrono::nanoseconds(10));
            continue;
        }

        if (all_task_groups_complete)
        {
            return;
        }

        if (all_task_groups_done)
        {
            return;
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::dynamicSleepingWorker(int thread_id)
{
    // Lock the task queue until you hear from the main thread that there is more work to do
    for (;;)
    {
        std::unique_lock<std::mutex> lock(*work_queue_mutex);

        if (all_task_groups_done)
        {
            lock.unlock();
            return;
        }

        // *only* wait if there's no work in the queue
        // otherwise, just work on the next task
        if (task_groups_to_complete.empty())
        {
            // sleep the thread
            lock.unlock();
            std::this_thread::sleep_for(std::chrono::nanoseconds(10));
            continue;
        }

        TaskGroup *task_to_do = task_groups_to_complete.front();

        // Save the current task
        int local_ctr = task_to_do->current_task_index;
        task_to_do->current_task_index += 1;

        // now that we've finished, mark complete if this was the final task
        // Also pop it out of the queue
        if (local_ctr == task_to_do->num_total_tasks)
        {
            task_to_do->complete = true;
            task_to_do->runnable->runTask(local_ctr, task_to_do->num_total_tasks);
            task_groups_to_complete.pop_front();
            work_done->notify_one();
        }
        else
        {
            // Run the task
            lock.unlock();
            task_to_do->runnable->runTask(local_ctr, task_to_do->num_total_tasks);
            lock.lock();
        }

        lock.unlock();
    }
}
