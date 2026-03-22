#include "thread_pool.h"

#include <pthread.h>
#include <deque>
#include <vector>
#include <time.h>
#include <errno.h>

struct thread_task {
    thread_task_f function;
    mutable pthread_mutex_t mutex;
    pthread_cond_t cond;
    struct thread_pool *pool;
    bool was_pushed;
    bool in_pool;
    bool running;
    bool finished;
    bool joined;
    bool detached;
};

struct thread_pool {
    std::vector<pthread_t> threads;
    std::deque<thread_task *> queue;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int max_threads;
    int idle_threads;
    int active_tasks;
    bool stop;
};

static void thread_task_destroy(struct thread_task *task)
{
    pthread_cond_destroy(&task->cond);
    pthread_mutex_destroy(&task->mutex);
    delete task;
}

static void *thread_pool_worker(void *arg)
{
    auto *pool = static_cast<thread_pool *>(arg);
    while (true) {
        pthread_mutex_lock(&pool->mutex);
        while (!pool->stop && pool->queue.empty()) {
            ++pool->idle_threads;
            pthread_cond_wait(&pool->cond, &pool->mutex);
            --pool->idle_threads;
        }
        if (pool->stop && pool->queue.empty()) {
            pthread_mutex_unlock(&pool->mutex);
            return nullptr;
        }
        thread_task *task = pool->queue.front();
        pool->queue.pop_front();
        pthread_mutex_unlock(&pool->mutex);

        pthread_mutex_lock(&task->mutex);
        task->running = true;
        pthread_mutex_unlock(&task->mutex);

        task->function();

        pthread_mutex_lock(&pool->mutex);
        --pool->active_tasks;
        pthread_mutex_unlock(&pool->mutex);

        bool should_delete = false;
        pthread_mutex_lock(&task->mutex);
        task->running = false;
        task->finished = true;
        task->in_pool = false;
        should_delete = task->detached;
        if (!should_delete)
            pthread_cond_broadcast(&task->cond);
        pthread_mutex_unlock(&task->mutex);

        if (should_delete)
            thread_task_destroy(task);
    }
}

int thread_pool_new(int thread_count, struct thread_pool **pool)
{
    if (thread_count <= 0 || thread_count > TPOOL_MAX_THREADS)
        return TPOOL_ERR_INVALID_ARGUMENT;

    auto *p = new thread_pool;
    pthread_mutex_init(&p->mutex, nullptr);
    pthread_cond_init(&p->cond, nullptr);
    p->max_threads = thread_count;
    p->idle_threads = 0;
    p->active_tasks = 0;
    p->stop = false;
    *pool = p;
    return 0;
}

int thread_pool_delete(struct thread_pool *pool)
{
    pthread_mutex_lock(&pool->mutex);
    if (pool->active_tasks > 0) {
        pthread_mutex_unlock(&pool->mutex);
        return TPOOL_ERR_HAS_TASKS;
    }
    pool->stop = true;
    pthread_cond_broadcast(&pool->cond);
    pthread_mutex_unlock(&pool->mutex);

    for (pthread_t tid : pool->threads)
        pthread_join(tid, nullptr);

    pthread_cond_destroy(&pool->cond);
    pthread_mutex_destroy(&pool->mutex);
    delete pool;
    return 0;
}

int thread_pool_push_task(struct thread_pool *pool, struct thread_task *task)
{
    pthread_mutex_lock(&task->mutex);

    thread_pool *old_pool = task->pool;
    bool old_was_pushed = task->was_pushed;
    bool old_finished = task->finished;
    bool old_joined = task->joined;

    if (task->in_pool || task->running || task->detached ||
        (task->was_pushed && !task->joined)) {
        pthread_mutex_unlock(&task->mutex);
        return TPOOL_ERR_TASK_IN_POOL;
    }

    task->pool = pool;
    task->was_pushed = true;
    task->in_pool = true;
    task->running = false;
    task->finished = false;
    task->joined = false;

    pthread_mutex_unlock(&task->mutex);

    pthread_mutex_lock(&pool->mutex);

    if (pool->active_tasks >= TPOOL_MAX_TASKS) {
        pthread_mutex_unlock(&pool->mutex);

        pthread_mutex_lock(&task->mutex);
        task->pool = old_pool;
        task->in_pool = false;
        task->was_pushed = old_was_pushed;
        task->finished = old_finished;
        task->joined = old_joined;
        pthread_mutex_unlock(&task->mutex);

        return TPOOL_ERR_TOO_MANY_TASKS;
    }

    pool->queue.push_back(task);
    ++pool->active_tasks;

    if (pool->idle_threads > 0) {
        pthread_cond_signal(&pool->cond);
    } else if ((int)pool->threads.size() < pool->max_threads) {
        pthread_t tid;
        if (pthread_create(&tid, nullptr, thread_pool_worker, pool) == 0)
            pool->threads.push_back(tid);
    }

    pthread_mutex_unlock(&pool->mutex);
    return 0;
}

int thread_task_new(struct thread_task **task, const thread_task_f &function)
{
    auto *t = new thread_task;
    t->function = function;
    pthread_mutex_init(&t->mutex, nullptr);
    pthread_cond_init(&t->cond, nullptr);
    t->pool = nullptr;
    t->was_pushed = false;
    t->in_pool = false;
    t->running = false;
    t->finished = false;
    t->joined = false;
    t->detached = false;
    *task = t;
    return 0;
}

bool thread_task_is_finished(const struct thread_task *task)
{
    pthread_mutex_lock(&task->mutex);
    bool res = task->finished;
    pthread_mutex_unlock(&task->mutex);
    return res;
}

bool thread_task_is_running(const struct thread_task *task)
{
    pthread_mutex_lock(&task->mutex);
    bool res = task->running;
    pthread_mutex_unlock(&task->mutex);
    return res;
}

int thread_task_join(struct thread_task *task)
{
    pthread_mutex_lock(&task->mutex);
    if (!task->was_pushed) {
        pthread_mutex_unlock(&task->mutex);
        return TPOOL_ERR_TASK_NOT_PUSHED;
    }
    while (!task->finished)
        pthread_cond_wait(&task->cond, &task->mutex);
    task->joined = true;
    pthread_mutex_unlock(&task->mutex);
    return 0;
}

#if NEED_TIMED_JOIN

int thread_task_timed_join(struct thread_task *task, double timeout)
{
    pthread_mutex_lock(&task->mutex);

    if (!task->was_pushed) {
        pthread_mutex_unlock(&task->mutex);
        return TPOOL_ERR_TASK_NOT_PUSHED;
    }

    if (task->finished) {
        task->joined = true;
        pthread_mutex_unlock(&task->mutex);
        return 0;
    }

    if (timeout <= 0) {
        pthread_mutex_unlock(&task->mutex);
        return TPOOL_ERR_TIMEOUT;
    }

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);

    long sec = (long)timeout;
    long nsec = (long)((timeout - (double)sec) * 1000000000.0);

    ts.tv_sec += sec;
    ts.tv_nsec += nsec;

    if (ts.tv_nsec >= 1000000000L) {
        ts.tv_sec += 1;
        ts.tv_nsec -= 1000000000L;
    }

    while (!task->finished) {
        int rc = pthread_cond_timedwait(&task->cond, &task->mutex, &ts);
        if (rc == ETIMEDOUT && !task->finished) {
            pthread_mutex_unlock(&task->mutex);
            return TPOOL_ERR_TIMEOUT;
        }
    }

    task->joined = true;
    pthread_mutex_unlock(&task->mutex);
    return 0;
}

#endif

int thread_task_delete(struct thread_task *task)
{
    pthread_mutex_lock(&task->mutex);

    bool can_delete = !task->detached &&
                      !task->in_pool &&
                      !task->running &&
                      (!task->was_pushed || task->joined);

    pthread_mutex_unlock(&task->mutex);

    if (!can_delete)
        return TPOOL_ERR_TASK_IN_POOL;

    thread_task_destroy(task);
    return 0;
}

#if NEED_DETACH

int thread_task_detach(struct thread_task *task)
{
    pthread_mutex_lock(&task->mutex);

    if (!task->was_pushed) {
        pthread_mutex_unlock(&task->mutex);
        return TPOOL_ERR_TASK_NOT_PUSHED;
    }

    if (task->finished) {
        pthread_mutex_unlock(&task->mutex);
        thread_task_destroy(task);
        return 0;
    }

    task->detached = true;

    pthread_mutex_unlock(&task->mutex);
    return 0;
}

#endif
