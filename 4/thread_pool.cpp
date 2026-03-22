#include "thread_pool.h"

#include <pthread.h>
#include <vector>
#include <queue>
#include <atomic>
#include <time.h>
#include <errno.h>

struct thread_task {
    thread_task_f function;
    pthread_mutex_t mutex;
    pthread_cond_t cond;

    struct thread_pool *pool;

    bool was_pushed;
    bool in_pool;
    bool running;
    bool finished;
    bool joined;
    bool detached;

    thread_task(const thread_task_f& f)
        : function(f), pool(nullptr),
          was_pushed(false), in_pool(false),
          running(false), finished(false),
          joined(false), detached(false) {
        pthread_mutex_init(&mutex, nullptr);
        pthread_cond_init(&cond, nullptr);
    }

    ~thread_task() {
        pthread_mutex_destroy(&mutex);
        pthread_cond_destroy(&cond);
    }
};

struct thread_pool {
    std::vector<pthread_t> threads;
    std::queue<thread_task*> queue;
    pthread_mutex_t mutex;
    pthread_cond_t cond;

    int max_threads;
    int idle_threads;
    int active_tasks;
    bool stop;

    thread_pool(int n)
        : max_threads(n), idle_threads(0),
          active_tasks(0), stop(false) {
        pthread_mutex_init(&mutex, nullptr);
        pthread_cond_init(&cond, nullptr);
    }

    ~thread_pool() {
        pthread_mutex_destroy(&mutex);
        pthread_cond_destroy(&cond);
    }

    static void destroy_task(thread_task *t) {
        delete t;
    }

    static void* worker(void *arg) {
        thread_pool *pool = (thread_pool*)arg;

        while (true) {
            pthread_mutex_lock(&pool->mutex);

            while (!pool->stop && pool->queue.empty()) {
                pool->idle_threads++;
                pthread_cond_wait(&pool->cond, &pool->mutex);
                pool->idle_threads--;
            }

            if (pool->stop && pool->queue.empty()) {
                pthread_mutex_unlock(&pool->mutex);
                return nullptr;
            }

            thread_task *task = pool->queue.front();
            pool->queue.pop();

            pthread_mutex_unlock(&pool->mutex);

            pthread_mutex_lock(&task->mutex);
            task->running = true;
            pthread_mutex_unlock(&task->mutex);

            task->function();

            pthread_mutex_lock(&pool->mutex);
            pool->active_tasks--;
            pthread_mutex_unlock(&pool->mutex);

            bool del = false;

            pthread_mutex_lock(&task->mutex);
            task->running = false;
            task->finished = true;
            task->in_pool = false;
            del = task->detached;
            if (!del)
                pthread_cond_broadcast(&task->cond);
            pthread_mutex_unlock(&task->mutex);

            if (del)
                destroy_task(task);
        }
    }

    void maybe_add_thread() {
        if (idle_threads > 0)
            return;
        if ((int)threads.size() >= max_threads)
            return;
        pthread_t t;
        if (pthread_create(&t, nullptr, worker, this) == 0)
            threads.push_back(t);
    }
};

int thread_pool_new(int thread_count, struct thread_pool **pool) {
    if (thread_count <= 0 || thread_count > TPOOL_MAX_THREADS)
        return TPOOL_ERR_INVALID_ARGUMENT;
    *pool = new thread_pool(thread_count);
    return 0;
}

int thread_pool_delete(struct thread_pool *pool) {
    if (!pool)
        return TPOOL_ERR_INVALID_ARGUMENT;

    pthread_mutex_lock(&pool->mutex);
    if (pool->active_tasks > 0) {
        pthread_mutex_unlock(&pool->mutex);
        return TPOOL_ERR_HAS_TASKS;
    }
    pool->stop = true;
    pthread_cond_broadcast(&pool->cond);
    pthread_mutex_unlock(&pool->mutex);

    for (auto &t : pool->threads)
        pthread_join(t, nullptr);

    delete pool;
    return 0;
}

int thread_pool_push_task(struct thread_pool *pool, struct thread_task *task) {
    if (!pool || !task)
        return TPOOL_ERR_INVALID_ARGUMENT;

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

    pool->queue.push(task);
    pool->active_tasks++;

    if (pool->idle_threads > 0)
        pthread_cond_signal(&pool->cond);
    else
        pool->maybe_add_thread();

    pthread_mutex_unlock(&pool->mutex);

    return 0;
}

int thread_task_new(struct thread_task **task, const thread_task_f &function) {
    *task = new thread_task(function);
    return 0;
}

bool thread_task_is_finished(const struct thread_task *task) {
    pthread_mutex_lock((pthread_mutex_t*)&task->mutex);
    bool r = task->finished;
    pthread_mutex_unlock((pthread_mutex_t*)&task->mutex);
    return r;
}

bool thread_task_is_running(const struct thread_task *task) {
    pthread_mutex_lock((pthread_mutex_t*)&task->mutex);
    bool r = task->running;
    pthread_mutex_unlock((pthread_mutex_t*)&task->mutex);
    return r;
}

int thread_task_join(struct thread_task *task) {
    if (!task)
        return TPOOL_ERR_INVALID_ARGUMENT;

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
int thread_task_timed_join(struct thread_task *task, double timeout) {
    if (!task)
        return TPOOL_ERR_INVALID_ARGUMENT;

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
    long nsec = (long)((timeout - (double)sec) * 1e9);

    ts.tv_sec += sec;
    ts.tv_nsec += nsec;

    if (ts.tv_nsec >= 1000000000L) {
        ts.tv_sec++;
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

int thread_task_delete(struct thread_task *task) {
    if (!task)
        return TPOOL_ERR_INVALID_ARGUMENT;

    pthread_mutex_lock(&task->mutex);

    bool ok = !task->detached &&
              !task->in_pool &&
              !task->running &&
              (!task->was_pushed || task->joined);

    pthread_mutex_unlock(&task->mutex);

    if (!ok)
        return TPOOL_ERR_TASK_IN_POOL;

    delete task;
    return 0;
}

#if NEED_DETACH
int thread_task_detach(struct thread_task *task) {
    if (!task)
        return TPOOL_ERR_INVALID_ARGUMENT;

    pthread_mutex_lock(&task->mutex);

    if (!task->was_pushed) {
        pthread_mutex_unlock(&task->mutex);
        return TPOOL_ERR_TASK_NOT_PUSHED;
    }

    if (task->finished) {
        pthread_mutex_unlock(&task->mutex);
        delete task;
        return 0;
    }

    task->detached = true;

    pthread_mutex_unlock(&task->mutex);
    return 0;
}
#endif
