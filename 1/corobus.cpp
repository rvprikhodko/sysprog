#include "corobus.h"

#include "libcoro.h"
#include "rlist.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <new>

struct wakeup_entry {
    struct rlist base;
    struct coro *coro;
};

struct wakeup_queue {
    struct rlist coros;
};

static void
wakeup_queue_init(struct wakeup_queue *queue)
{
    rlist_create(&queue->coros);
}

static void
wakeup_queue_suspend_this(struct wakeup_queue *queue)
{
    struct wakeup_entry entry;
    entry.coro = coro_this();
    rlist_create(&entry.base);
    rlist_add_tail_entry(&queue->coros, &entry, base);
    coro_suspend();
    if (!rlist_empty(&entry.base))
        rlist_del_entry(&entry, base);
}

static void
wakeup_queue_wakeup_first(struct wakeup_queue *queue)
{
    if (rlist_empty(&queue->coros))
        return;
    struct wakeup_entry *entry = rlist_first_entry(&queue->coros,
        struct wakeup_entry, base);
    rlist_del(&entry->base);
    coro_wakeup(entry->coro);
}

static void
wakeup_queue_wakeup_all(struct wakeup_queue *queue)
{
    while (!rlist_empty(&queue->coros))
        wakeup_queue_wakeup_first(queue);
}

struct message_queue {
    unsigned *buffer;
    size_t capacity;
    size_t size;
    size_t head;
    size_t tail;
};

static bool
message_queue_init(struct message_queue *queue, size_t capacity)
{
    if (capacity == 0)
        return false;
    
    queue->buffer = new unsigned[capacity];
    if (!queue->buffer)
        return false;
    
    queue->capacity = capacity;
    queue->size = 0;
    queue->head = 0;
    queue->tail = 0;
    return true;
}

static void
message_queue_destroy(struct message_queue *queue)
{
    delete[] queue->buffer;
    queue->buffer = NULL;
    queue->capacity = 0;
    queue->size = 0;
    queue->head = 0;
    queue->tail = 0;
}

static bool
message_queue_is_empty(const struct message_queue *queue)
{
    return queue->size == 0;
}

static bool
message_queue_is_full(const struct message_queue *queue)
{
    return queue->size == queue->capacity;
}

static bool
message_queue_push(struct message_queue *queue, unsigned data)
{
    if (message_queue_is_full(queue))
        return false;
    
    queue->buffer[queue->tail] = data;
    queue->tail = (queue->tail + 1) % queue->capacity;
    queue->size++;
    return true;
}

static bool
message_queue_pop(struct message_queue *queue, unsigned *data)
{
    if (message_queue_is_empty(queue))
        return false;
    
    *data = queue->buffer[queue->head];
    queue->head = (queue->head + 1) % queue->capacity;
    queue->size--;
    return true;
}

#if NEED_BATCH

/** Push multiple messages to the queue. Returns number of messages pushed. */
static size_t
message_queue_push_many(struct message_queue *queue, 
                       const unsigned *data, size_t count)
{
    if (message_queue_is_full(queue) || count == 0)
        return 0;
    
    size_t free_space = queue->capacity - queue->size;
    size_t to_push = count < free_space ? count : free_space;
    
    if (to_push == 0)
        return 0;
    
    size_t first_part = queue->capacity - queue->tail;
    size_t first_push = to_push < first_part ? to_push : first_part;
    
    for (size_t i = 0; i < first_push; ++i) {
        queue->buffer[queue->tail + i] = data[i];
    }
    
    size_t second_push = to_push - first_push;
    for (size_t i = 0; i < second_push; ++i) {
        queue->buffer[i] = data[first_push + i];
    }
    
    queue->tail = (queue->tail + to_push) % queue->capacity;
    queue->size += to_push;
    return to_push;
}

/** Pop multiple messages from the queue. Returns number of messages popped. */
static size_t
message_queue_pop_many(struct message_queue *queue,
                      unsigned *data, size_t capacity)
{
    if (message_queue_is_empty(queue) || capacity == 0)
        return 0;
    
    size_t to_pop = queue->size < capacity ? queue->size : capacity;
    
    if (to_pop == 0)
        return 0;
    
    // Две части: от head до конца буфера и с начала буфера
    size_t first_part = queue->capacity - queue->head;
    size_t first_pop = to_pop < first_part ? to_pop : first_part;
    
    for (size_t i = 0; i < first_pop; ++i) {
        data[i] = queue->buffer[queue->head + i];
    }
    
    size_t second_pop = to_pop - first_pop;
    for (size_t i = 0; i < second_pop; ++i) {
        data[first_pop + i] = queue->buffer[i];
    }
    
    queue->head = (queue->head + to_pop) % queue->capacity;
    queue->size -= to_pop;
    return to_pop;
}

#endif /* NEED_BATCH */

struct coro_bus_channel {
    size_t size_limit;
    struct message_queue messages;
    struct wakeup_queue send_queue;
    struct wakeup_queue recv_queue;
};

struct coro_bus {
    struct coro_bus_channel **channels;
    int channel_count;
};

static enum coro_bus_error_code global_error = CORO_BUS_ERR_NONE;

enum coro_bus_error_code
coro_bus_errno(void)
{
    return global_error;
}

void
coro_bus_errno_set(enum coro_bus_error_code err)
{
    global_error = err;
}

struct coro_bus *
coro_bus_new(void)
{
    struct coro_bus *bus = new(std::nothrow) struct coro_bus;
    if (!bus) {
        coro_bus_errno_set(CORO_BUS_ERR_NONE);
        return NULL;
    }
    
    bus->channels = NULL;
    bus->channel_count = 0;
    
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    return bus;
}

void
coro_bus_delete(struct coro_bus *bus)
{
    if (!bus)
        return;
    
    for (int i = 0; i < bus->channel_count; ++i) {
        if (bus->channels[i]) {
            coro_bus_channel_close(bus, i);
        }
    }
    
    delete[] bus->channels;
    delete bus;
}

int
coro_bus_channel_open(struct coro_bus *bus, size_t size_limit)
{
    if (!bus) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }
    
    for (int i = 0; i < bus->channel_count; ++i) {
        if (bus->channels[i] == NULL) {
            struct coro_bus_channel *channel = new(std::nothrow) struct coro_bus_channel;
            if (!channel) {
                coro_bus_errno_set(CORO_BUS_ERR_NONE);
                return -1;
            }
            
            channel->size_limit = size_limit;
            if (!message_queue_init(&channel->messages, size_limit)) {
                delete channel;
                coro_bus_errno_set(CORO_BUS_ERR_NONE);
                return -1;
            }
            
            wakeup_queue_init(&channel->send_queue);
            wakeup_queue_init(&channel->recv_queue);
            
            bus->channels[i] = channel;
            coro_bus_errno_set(CORO_BUS_ERR_NONE);
            return i;
        }
    }
    
    int new_count = bus->channel_count + 1;
    struct coro_bus_channel **new_channels = new(std::nothrow) struct coro_bus_channel*[new_count];
    if (!new_channels) {
        coro_bus_errno_set(CORO_BUS_ERR_NONE);
        return -1;
    }
    
    for (int i = 0; i < bus->channel_count; ++i) {
        new_channels[i] = bus->channels[i];
    }
    
    struct coro_bus_channel *channel = new(std::nothrow) struct coro_bus_channel;
    if (!channel) {
        delete[] new_channels;
        coro_bus_errno_set(CORO_BUS_ERR_NONE);
        return -1;
    }
    
    channel->size_limit = size_limit;
    if (!message_queue_init(&channel->messages, size_limit)) {
        delete channel;
        delete[] new_channels;
        coro_bus_errno_set(CORO_BUS_ERR_NONE);
        return -1;
    }
    
    wakeup_queue_init(&channel->send_queue);
    wakeup_queue_init(&channel->recv_queue);
    
    new_channels[bus->channel_count] = channel;
    
    delete[] bus->channels;
    bus->channels = new_channels;
    bus->channel_count = new_count;
    
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    return bus->channel_count - 1;
}

void
coro_bus_channel_close(struct coro_bus *bus, int channel)
{
    if (!bus || channel < 0 || channel >= bus->channel_count || !bus->channels[channel]) {
        return;
    }
    
    struct coro_bus_channel *ch = bus->channels[channel];
    
    bus->channels[channel] = NULL;
    
    while (!rlist_empty(&ch->send_queue.coros)) {
        struct wakeup_entry *entry = rlist_first_entry(&ch->send_queue.coros,
            struct wakeup_entry, base);
        rlist_del(&entry->base);
        coro_wakeup(entry->coro);
    }
    
    while (!rlist_empty(&ch->recv_queue.coros)) {
        struct wakeup_entry *entry = rlist_first_entry(&ch->recv_queue.coros,
            struct wakeup_entry, base);
        rlist_del(&entry->base);
        coro_wakeup(entry->coro);
    }
    
    message_queue_destroy(&ch->messages);

    delete ch;
    
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
}

static struct coro_bus_channel *
get_channel(struct coro_bus *bus, int channel)
{
    if (!bus || channel < 0 || channel >= bus->channel_count) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return NULL;
    }
    
    struct coro_bus_channel *ch = bus->channels[channel];
    if (!ch) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return NULL;
    }
    
    return ch;
}

int
coro_bus_try_send(struct coro_bus *bus, int channel, unsigned data)
{
    struct coro_bus_channel *ch = get_channel(bus, channel);
    if (!ch)
        return -1;
    
    if (message_queue_is_full(&ch->messages)) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }
    
    if (!message_queue_push(&ch->messages, data)) {
        coro_bus_errno_set(CORO_BUS_ERR_NONE);
        return -1;
    }
    
    wakeup_queue_wakeup_first(&ch->recv_queue);
    
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    return 0;
}

int
coro_bus_send(struct coro_bus *bus, int channel, unsigned data)
{
    while (true) {
        struct coro_bus_channel *ch = get_channel(bus, channel);
        if (!ch)
            return -1;
        
        if (!message_queue_is_full(&ch->messages)) {
            if (message_queue_push(&ch->messages, data)) {
                wakeup_queue_wakeup_first(&ch->recv_queue);
                coro_bus_errno_set(CORO_BUS_ERR_NONE);
                return 0;
            }
        }
        
        wakeup_queue_suspend_this(&ch->send_queue);
        
        if (!get_channel(bus, channel)) {
            return -1;
        }
    }
}


int
coro_bus_try_recv(struct coro_bus *bus, int channel, unsigned *data)
{
    struct coro_bus_channel *ch = get_channel(bus, channel);
    if (!ch)
        return -1;
    
    if (message_queue_is_empty(&ch->messages)) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }
    
    if (!message_queue_pop(&ch->messages, data)) {
        coro_bus_errno_set(CORO_BUS_ERR_NONE);
        return -1;
    }
    
    wakeup_queue_wakeup_first(&ch->send_queue);
    
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    return 0;
}

int
coro_bus_recv(struct coro_bus *bus, int channel, unsigned *data)
{
    while (true) {
        struct coro_bus_channel *ch = get_channel(bus, channel);
        if (!ch)
            return -1;
        
        if (!message_queue_is_empty(&ch->messages)) {
            if (message_queue_pop(&ch->messages, data)) {
                wakeup_queue_wakeup_first(&ch->send_queue);
                coro_bus_errno_set(CORO_BUS_ERR_NONE);
                return 0;
            }
        }
        
        wakeup_queue_suspend_this(&ch->recv_queue);
        
        if (!get_channel(bus, channel)) {
            return -1;
        }
    }
}


#if NEED_BROADCAST

int
coro_bus_try_broadcast(struct coro_bus *bus, unsigned data)
{
    if (!bus) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }
    
    bool has_channels = false;
    bool any_full = false;
    
    for (int i = 0; i < bus->channel_count; ++i) {
        struct coro_bus_channel *ch = bus->channels[i];
        if (ch) {
            has_channels = true;
            if (message_queue_is_full(&ch->messages)) {
                any_full = true;
                break;
            }
        }
    }
    
    if (!has_channels) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }
    
    if (any_full) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }
    
    for (int i = 0; i < bus->channel_count; ++i) {
        struct coro_bus_channel *ch = bus->channels[i];
        if (ch) {
            message_queue_push(&ch->messages, data);
            wakeup_queue_wakeup_first(&ch->recv_queue);
        }
    }
    
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    return 0;
}

int
coro_bus_broadcast(struct coro_bus *bus, unsigned data)
{
    if (!bus) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }
    
    bool has_channels = false;
    for (int i = 0; i < bus->channel_count; ++i) {
        if (bus->channels[i]) {
            has_channels = true;
            break;
        }
    }
    
    if (!has_channels) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }
    
    while (true) {
        has_channels = false;
        for (int i = 0; i < bus->channel_count; ++i) {
            if (bus->channels[i]) {
                has_channels = true;
                break;
            }
        }
        
        if (!has_channels) {
            coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
            return -1;
        }
        
        int rc = coro_bus_try_broadcast(bus, data);
        if (rc == 0) {
            return 0;
        }
        
        if (coro_bus_errno() == CORO_BUS_ERR_WOULD_BLOCK) {
            coro_yield();
        } else {
            return -1;
        }
    }
}

#endif /* NEED_BROADCAST */

#if NEED_BATCH

int
coro_bus_try_send_v(struct coro_bus *bus, int channel,
    const unsigned *data, unsigned count)
{
    struct coro_bus_channel *ch = get_channel(bus, channel);
    if (!ch)
        return -1;
    
    if (count == 0) {
        coro_bus_errno_set(CORO_BUS_ERR_NONE);
        return 0;
    }
    
    if (message_queue_is_full(&ch->messages)) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }
    
    size_t sent = message_queue_push_many(&ch->messages, data, count);
    
    if (sent > 0)
    	wakeup_queue_wakeup_all(&ch->recv_queue);

    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    return (int)sent;
}

int
coro_bus_send_v(struct coro_bus *bus, int channel,
    const unsigned *data, unsigned count)
{
    if (count == 0) {
        coro_bus_errno_set(CORO_BUS_ERR_NONE);
        return 0;
    }

    while (true) {
        struct coro_bus_channel *ch = get_channel(bus, channel);
        if (!ch)
            return -1;

        int sent = coro_bus_try_send_v(bus, channel, data, count);

        if (sent >= 0) {
            coro_bus_errno_set(CORO_BUS_ERR_NONE);
            return sent;
        }

        if (coro_bus_errno() == CORO_BUS_ERR_WOULD_BLOCK) {
            wakeup_queue_suspend_this(&ch->send_queue);
            if (!get_channel(bus, channel)) {
                return -1;
            }
        } else {
            return -1;
        }
    }
}


int
coro_bus_try_recv_v(struct coro_bus *bus, int channel,
    unsigned *data, unsigned capacity)
{
    struct coro_bus_channel *ch = get_channel(bus, channel);
    if (!ch)
        return -1;
    
    if (capacity == 0) {
        coro_bus_errno_set(CORO_BUS_ERR_NONE);
        return 0;
    }
    
    if (message_queue_is_empty(&ch->messages)) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }
    
    size_t received = message_queue_pop_many(&ch->messages, data, capacity);
    
    if (received > 0)
    	wakeup_queue_wakeup_all(&ch->send_queue);

    
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    return (int)received;
}

int
coro_bus_recv_v(struct coro_bus *bus, int channel,
    unsigned *data, unsigned capacity)
{
    if (capacity == 0) {
        coro_bus_errno_set(CORO_BUS_ERR_NONE);
        return 0;
    }

    while (true) {
        struct coro_bus_channel *ch = get_channel(bus, channel);
        if (!ch)
            return -1;

        int received = coro_bus_try_recv_v(bus, channel, data, capacity);

        if (received >= 0) {
            coro_bus_errno_set(CORO_BUS_ERR_NONE);
            return received;
        }

        if (coro_bus_errno() == CORO_BUS_ERR_WOULD_BLOCK) {
            wakeup_queue_suspend_this(&ch->recv_queue);
            
            if (!get_channel(bus, channel)) {
                return -1;
            }
        } else {
            return -1;
        }
    }
}


#endif /* NEED_BATCH */
