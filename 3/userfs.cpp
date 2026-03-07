#include "userfs.h"
#include "rlist.h"

#include <stddef.h>
#include <string>
#include <vector>
#include <cstring>
#include <algorithm>

enum {
    BLOCK_SIZE = 512,
    MAX_FILE_SIZE = 1024 * 1024 * 100,
};

static ufs_error_code ufs_error_code = UFS_ERR_NO_ERR;

struct block {
    char memory[BLOCK_SIZE];
    rlist in_block_list = RLIST_LINK_INITIALIZER;
};

struct file {
    rlist blocks = RLIST_HEAD_INITIALIZER(blocks);
    int refs = 0;
    std::string name;
    rlist in_file_list = RLIST_LINK_INITIALIZER;

    size_t size = 0;
    bool deleted = false;

    std::vector<block*> block_index;
};

static rlist file_list = RLIST_HEAD_INITIALIZER(file_list);

struct filedesc {
    file *atfile;
    size_t pos = 0;
    int flags;
};

static std::vector<filedesc*> file_descriptors;

enum ufs_error_code
ufs_errno()
{
    return ufs_error_code;
}

static file*
find_file(const char *filename)
{
    file *f;
    rlist_foreach_entry(f, &file_list, in_file_list) {
        if (!f->deleted && f->name == filename)
            return f;
    }
    return NULL;
}

static block*
block_at(file *f, size_t index)
{
    if (index >= f->block_index.size())
        return NULL;
    return f->block_index[index];
}

static block*
ensure_block(file *f, size_t index)
{
    size_t count = f->block_index.size();

    while (count <= index) {

        block *b = new block;
        memset(b->memory, 0, BLOCK_SIZE);

        rlist_add_tail_entry(&f->blocks, b, in_block_list);
        f->block_index.push_back(b);

        count++;
    }

    return block_at(f, index);
}

static void
free_blocks(file *f)
{
    for (size_t i = 0; i < f->block_index.size(); i++) {

        block *b = f->block_index[i];

        rlist_del_entry(b, in_block_list);
        delete b;
    }

    f->block_index.clear();
}

static int
alloc_fd(filedesc *desc)
{
    for (size_t i = 0; i < file_descriptors.size(); i++) {

        if (file_descriptors[i] == NULL) {

            file_descriptors[i] = desc;
            return (int)i + 1;
        }
    }

    file_descriptors.push_back(desc);
    return (int)file_descriptors.size();
}

int
ufs_open(const char *filename, int flags)
{
    file *f = find_file(filename);

    if (f == NULL) {

        if (!(flags & UFS_CREATE)) {
            ufs_error_code = UFS_ERR_NO_FILE;
            return -1;
        }

        f = new file();
        f->name = filename;

        rlist_add_tail_entry(&file_list, f, in_file_list);
    }

#if NEED_OPEN_FLAGS

    if (!(flags & (UFS_READ_ONLY | UFS_WRITE_ONLY))) {
        flags |= UFS_READ_WRITE;
    }

#endif

    filedesc *desc = new filedesc;
    desc->atfile = f;
    desc->pos = 0;
    desc->flags = flags;

    f->refs++;

    ufs_error_code = UFS_ERR_NO_ERR;

    return alloc_fd(desc);
}
ssize_t
ufs_write(int fd, const char *buf, size_t size)
{
    if (fd <= 0 || (size_t)fd > file_descriptors.size() ||
        file_descriptors[fd - 1] == NULL) {

        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }

    if (size == 0)
        return 0;

    filedesc *desc = file_descriptors[fd - 1];
    file *f = desc->atfile;
    
#if NEED_OPEN_FLAGS
    if (!(desc->flags & UFS_WRITE_ONLY)) {
        ufs_error_code = UFS_ERR_NO_PERMISSION;
        return -1;
    }
#endif

    if (desc->pos + size > MAX_FILE_SIZE) {

        ufs_error_code = UFS_ERR_NO_MEM;
        return -1;
    }

    size_t end_pos = desc->pos + size;

    size_t last_block = (end_pos - 1) / BLOCK_SIZE;

    ensure_block(f, last_block);

    size_t written = 0;

    while (written < size) {

        size_t pos = desc->pos;

        size_t idx = pos / BLOCK_SIZE;
        size_t off = pos % BLOCK_SIZE;

        block *b = block_at(f, idx);

        size_t chunk = std::min(size - written, BLOCK_SIZE - off);

        memcpy(b->memory + off, buf + written, chunk);

        desc->pos += chunk;
        written += chunk;
    }

    if (end_pos > f->size)
        f->size = end_pos;

    ufs_error_code = UFS_ERR_NO_ERR;

    return written;
}

ssize_t
ufs_read(int fd, char *buf, size_t size)
{
    if (fd <= 0 || (size_t)fd > file_descriptors.size() ||
        file_descriptors[fd - 1] == NULL) {

        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }

    filedesc *desc = file_descriptors[fd - 1];
    file *f = desc->atfile;
    
#if NEED_OPEN_FLAGS
    if (!(desc->flags & UFS_READ_ONLY)) {
    	ufs_error_code = UFS_ERR_NO_PERMISSION;
    	return -1;
}
#endif

    if (desc->pos >= f->size)
        return 0;

    size_t to_read = std::min(size, f->size - desc->pos);

    size_t read = 0;

    while (read < to_read) {

        size_t pos = desc->pos;

        size_t idx = pos / BLOCK_SIZE;
        size_t off = pos % BLOCK_SIZE;

        block *b = block_at(f, idx);

        size_t chunk = std::min(to_read - read, BLOCK_SIZE - off);

        memcpy(buf + read, b->memory + off, chunk);

        desc->pos += chunk;
        read += chunk;
    }

    ufs_error_code = UFS_ERR_NO_ERR;

    return read;
}

int
ufs_close(int fd)
{
    if (fd <= 0 || (size_t)fd > file_descriptors.size() ||
        file_descriptors[fd - 1] == NULL) {

        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }

    filedesc *desc = file_descriptors[fd - 1];

    file_descriptors[fd - 1] = NULL;

    file *f = desc->atfile;

    delete desc;

    f->refs--;

    if (f->deleted && f->refs == 0) {

        rlist_del_entry(f, in_file_list);
        free_blocks(f);

        delete f;
    }

    ufs_error_code = UFS_ERR_NO_ERR;

    return 0;
}

int
ufs_delete(const char *filename)
{
    file *f = find_file(filename);

    if (f == NULL) {

        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }

    f->deleted = true;

    if (f->refs == 0) {

        rlist_del_entry(f, in_file_list);
        free_blocks(f);

        delete f;
    }

    ufs_error_code = UFS_ERR_NO_ERR;

    return 0;
}

#if NEED_RESIZE

int
ufs_resize(int fd, size_t new_size)
{
    if (fd <= 0 || (size_t)fd > file_descriptors.size() ||
        file_descriptors[fd - 1] == NULL) {

        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }

    filedesc *desc = file_descriptors[fd - 1];
    file *f = desc->atfile;

#if NEED_OPEN_FLAGS
    if (!(desc->flags & UFS_WRITE_ONLY)) {
        ufs_error_code = UFS_ERR_NO_PERMISSION;
        return -1;
    }
#endif

    if (new_size > MAX_FILE_SIZE) {
        ufs_error_code = UFS_ERR_NO_MEM;
        return -1;
    }

    size_t old_size = f->size;

    if (new_size > old_size) {

        size_t last_block = (new_size - 1) / BLOCK_SIZE;

        ensure_block(f, last_block);

    } else if (new_size < old_size) {

        size_t needed_blocks =
            new_size == 0 ? 0 : ((new_size - 1) / BLOCK_SIZE) + 1;

        while (f->block_index.size() > needed_blocks) {

            block *b = f->block_index.back();

            rlist_del_entry(b, in_block_list);
            delete b;

            f->block_index.pop_back();
        }

        for (size_t i = 0; i < file_descriptors.size(); i++) {

            filedesc *d = file_descriptors[i];

            if (d != NULL && d->atfile == f && d->pos > new_size)
                d->pos = new_size;
        }
    }

    f->size = new_size;

    ufs_error_code = UFS_ERR_NO_ERR;

    return 0;
}

#endif

void
ufs_destroy(void)
{
    for (size_t i = 0; i < file_descriptors.size(); i++) {

        if (file_descriptors[i] == NULL)
            continue;

        delete file_descriptors[i];
        file_descriptors[i] = NULL;
    }

    file *f, *tmp;

    rlist_foreach_entry_safe(f, &file_list, in_file_list, tmp) {

        rlist_del_entry(f, in_file_list);
        free_blocks(f);

        delete f;
    }

    std::vector<filedesc*> empty;
    file_descriptors.swap(empty);
}
