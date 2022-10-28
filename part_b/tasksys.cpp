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
    std::cout << "deleting" << std::endl;
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
        deps
    );

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
    for (;;) {
        std::unique_lock<std::mutex> lock(*sync_mutex);
        bool all_task_groups_complete = true;
        // lock.unlock();
        
        // https: // stackoverflow.com/questions/26281979/c-loop-through-map
        for (const auto &[task_id, task] : task_groups)
        {   
            // lock.lock();
            if (task->complete) {
                std::cout << "Sync: \n\tTask ID: " << task->task_id << "Complete" << std::endl;
                continue;
            }  
            // lock.unlock();
            
            all_task_groups_complete = false;
            std::cout << "All Tasks Complete Status: " << all_task_groups_complete << std::endl;

            // loop through dependencies to see if they are done
            bool all_deps_ready = true;
            for (const auto dep_id : task->dep_list)
            {
                std::cout << "Sync: \n\tDependencies for Current Task: " << task_groups[dep_id]->task_id << std::endl;
                // lock.lock();
                if (task_groups[dep_id]->complete) {
                    continue;
                }
                // lock.unlock();
                all_deps_ready = false;
            }

            // if all deps ready and not launched, push it into the work queue and mark it launched
            // lock.lock();
            if (all_deps_ready && !task->launched) {
                std::cout << "Sync: \n\tLaunching Task " << task->task_id << std::endl;
                task->launched = true;
                task_groups_to_complete.push_back(task);
                work_ready->notify_all();

            }
            // lock.unlock();
        }

        std::cout << "Sync: \n\tWaiting on the work to be complete!" << std::endl;
        // lock.lock();
        if (!task_groups_to_complete.empty() || !all_task_groups_complete)
        {
            work_done->wait(lock);
            std::cout << "Sync: \n\tHeard back finding more tasks to do" << std::endl;
        } 
        // lock.unlock();

        if (all_task_groups_complete) {
            return;
        }

        // lock.lock();
        if (all_task_groups_done) {
            return;
        }
        // lock.unlock();
    } 
}

void TaskSystemParallelThreadPoolSleeping::dynamicSleepingWorker(int thread_id) {
    int local_counter = -1;
    // Lock the task queue until you hear from the main thread that there is more work to do
    for (;;) {
        std::unique_lock<std::mutex> lock(*work_queue_mutex);

        // *only* wait if there's no work in the queue
        // otherwise, just work on the next task
        if (task_groups_to_complete.empty() || !all_task_groups_done)
        {
            std::cout << "Worker " << thread_id << ": \n\tWaiting on main thread to give tasks" << std::endl;
            work_ready->wait(lock);
            std::cout << "Worker " << thread_id << ": \n\tMoved past waiting!" << std::endl;
        }

        if (all_task_groups_done) {
            std::cout << "Returning" << std::endl;
            lock.unlock();
            return;
        }
        // lock.unlock();

        // lock the task queue and read the current
        // lock.lock();
        auto task_to_do = task_groups_to_complete.front();
        // if (task_to_do->complete){
        //     task_groups_to_complete.pop_front();
        //     auto task_to_do = task_groups_to_complete.front();
        // }
        // lock.unlock();

        // Save the current task
        // lock.lock();
        int local_ctr = task_to_do->current_task_index;
        task_to_do->current_task_index += 1;
        // lock.unlock();

        // now that we've finished, mark complete if this was the final task
        // Also pop it out of the queue
        // lock.lock();
        if (local_ctr == task_to_do->num_total_tasks) {
            std::cout << "Worker " << thread_id << ": \n\tScheduling Task: " << local_ctr << "/" << task_to_do->num_total_tasks << "\n\tFor Task: " << task_to_do->task_id << std::endl;
            std::cout << "\tJust completed task group : " << task_to_do->task_id << std::endl;
            task_to_do->complete = true;
            task_to_do->runnable->runTask(local_ctr, task_to_do->num_total_tasks);
            task_groups_to_complete.pop_front();

            // lock.unlock();
            std::cout << "\tNotifying main thread" << std::endl;
            work_done->notify_one();
            
            // worker_lock.lock();
        }
        else {
            // Run the task
            std::cout << "Worker " << thread_id << ": \n\tScheduling Task: " << local_ctr << "/" << task_to_do->num_total_tasks << "\n\tFor Task: " << task_to_do->task_id << std::endl;
            lock.unlock();
            task_to_do->runnable->runTask(local_ctr, task_to_do->num_total_tasks);
            lock.lock();
        }
        
        lock.unlock();
    }
}
