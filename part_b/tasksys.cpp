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

    // std::cout << "New threadpool" << thread_total_num << std::endl;

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

    // std::cout << "Goodbye threadpool" << std::endl;
    delete work_queue_mutex;
    delete[] workers;
    delete work_ready;
    delete work_done;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable, int num_total_tasks)
{
    if (!running)
    {
        running = true;
        runAsyncWithDeps(runnable, num_total_tasks, std::vector<TaskID>());
        sync();
    }

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
    // some simple logging code to help debug the graph layouts
    // std::cout << "you called sync!" << std::endl;
    // for (const auto [task_id, task] : task_groups)
    // {
    //     std::cout << "[" << task->task_id << "](" << task->num_total_tasks << "): ";
    //     for (const auto dep : task->dep_list)
    //     {
    //         std::cout << dep << ", ";
    //     }
    //     std::cout << std::endl;
    // }

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
            if (task->complete->load())
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
                if (task_groups[dep_id]->complete->load())
                {
                    continue;
                }

                all_deps_ready = false;
            }

            // if all deps ready and not launched, push it into the work queue and mark it launched
            if (all_deps_ready && !(task->launched))
            {
                // std::cout << "Launching task group" << task->task_id << std::endl;
                task->launched = true;

                for (int x = 0; x < task->num_total_tasks; x++)
                {

                    tasks_pending.push_back(TaskUnit{
                        task->task_id,
                        x});

                    work_ready->notify_all();
                }
            }
        }

        lock.unlock();
        std::this_thread::sleep_for(std::chrono::nanoseconds(10));

        if (all_task_groups_complete)
        {
            all_task_groups_done = true;
            work_ready->notify_all();
            return;
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::dynamicSleepingWorker(int thread_id)
{
    // Lock the task queue until you hear from the main thread that there is more work to do
    // Pop out just a simple little unit of work
    for (;;)
    {
        std::unique_lock<std::mutex> work_lock(*work_queue_mutex);
        work_ready->wait(work_lock, [this]
                         { return !tasks_pending.empty() || all_task_groups_done; });

        if (all_task_groups_done)
        {
            work_lock.unlock();
            return;
        }

        TaskUnit task_to_do = tasks_pending.front();
        tasks_pending.pop_front();

        work_lock.unlock();

        IRunnable *runnable = task_groups[task_to_do.task]->runnable;

        // Run the task
        runnable->runTask(task_to_do.task_index, task_groups[task_to_do.task]->num_total_tasks);
        // std::cout << "task done !" << task_to_do.task_index << ", " << task_to_do.task << std::endl;

        task_groups[task_to_do.task]->tasks_complete->fetch_add(1);

        if (task_groups[task_to_do.task]->tasks_complete->load() == task_groups[task_to_do.task]->num_total_tasks)
        {

            task_groups[task_to_do.task]->complete->store(true);
        }
    }
}
