#include "parser.h"

#include <cassert>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <vector>
#include <string>

////////// BUILT INS //////////

static bool is_builtin(const command& cmd) {
    return cmd.exe == "cd" || cmd.exe == "exit";
}

static int run_builtin(const command& cmd, int last_status) {
    if (cmd.exe == "cd") {
        const char* path =
            cmd.args.empty() ? getenv("HOME")
                             : cmd.args[0].c_str();
        if (!path) path = ".";
        if (chdir(path) != 0) {
            perror("cd");
            return 1;
        }
        return 0;
    }

    if (cmd.exe == "exit") {
        if (cmd.args.empty())
            return last_status;
        return atoi(cmd.args[0].c_str()) & 0xFF;
    }

    return 0;
}

////////// EXEC ONE COMMAND //////////

static void exec_command(const command& cmd,
                         int in_fd,
                         int out_fd,
                         int last_status)
{
    if (in_fd != STDIN_FILENO) {
        dup2(in_fd, STDIN_FILENO);
        close(in_fd);
    }

    if (out_fd != STDOUT_FILENO) {
        dup2(out_fd, STDOUT_FILENO);
        close(out_fd);
    }

    if (is_builtin(cmd)) {
        int status = run_builtin(cmd, last_status);
        _exit(status);
    }

    std::vector<char*> argv;
    argv.push_back(const_cast<char*>(cmd.exe.c_str()));
    for (auto& a : cmd.args)
        argv.push_back(const_cast<char*>(a.c_str()));
    argv.push_back(nullptr);

    execvp(cmd.exe.c_str(), argv.data());
    perror(cmd.exe.c_str());
    _exit(127);
}

////////// PIPELINE //////////

static int execute_pipeline(const std::vector<const command*>& cmds,
                            const command_line* line,
                            int last_status,
                            bool& should_exit)
{
    if (cmds.empty())
        return 0;

    if (!line->is_background &&
        cmds.size() == 1 &&
        cmds[0]->exe == "exit")
    {
        should_exit = true;
        return run_builtin(*cmds[0], last_status);
    }

    if (!line->is_background &&
        cmds.size() == 1 &&
        cmds[0]->exe == "cd")
    {
        return run_builtin(*cmds[0], last_status);
    }

    std::vector<pid_t> pids;
    int prev_pipe[2] = {-1, -1};
    int status = 0;

    for (size_t i = 0; i < cmds.size(); ++i) {

        bool last = (i == cmds.size() - 1);
        int next_pipe[2] = {-1, -1};

        if (!last && pipe(next_pipe) < 0) {
            perror("pipe");
            return 1;
        }

        pid_t pid = fork();
        if (pid < 0) {
            perror("fork");
            return 1;
        }

        if (pid == 0) {

            int in_fd  = (i == 0) ? STDIN_FILENO : prev_pipe[0];
            int out_fd = STDOUT_FILENO;

            if (!last) {
                out_fd = next_pipe[1];
            } else if (!line->out_file.empty()) {
                int flags = O_CREAT | O_WRONLY;
                flags |= (line->out_type == OUTPUT_TYPE_FILE_APPEND)
                         ? O_APPEND : O_TRUNC;

                int fd = open(line->out_file.c_str(), flags, 0644);
                if (fd < 0) {
                    perror("open");
                    _exit(1);
                }
                out_fd = fd;
            }

            if (prev_pipe[0] != -1)
                close(prev_pipe[1]);

            if (!last)
                close(next_pipe[0]);

            exec_command(*cmds[i], in_fd, out_fd, last_status);
        }

        pids.push_back(pid);

        if (prev_pipe[0] != -1) {
            close(prev_pipe[0]);
            close(prev_pipe[1]);
        }

        prev_pipe[0] = next_pipe[0];
        prev_pipe[1] = next_pipe[1];
    }

    if (prev_pipe[0] != -1) {
        close(prev_pipe[0]);
        close(prev_pipe[1]);
    }

    if (!line->is_background) {
        for (size_t i = 0; i < pids.size(); ++i) {
            int wstatus;
            waitpid(pids[i], &wstatus, 0);

            if (i == pids.size() - 1) {
                if (WIFEXITED(wstatus))
                    status = WEXITSTATUS(wstatus);
                else if (WIFSIGNALED(wstatus))
                    status = 128 + WTERMSIG(wstatus);
            }
        }
    }

    return status;
}

////////// EXEC COMMAND LINE //////////

static int execute_command_line(const command_line* line,
                                int& last_status,
                                bool& should_exit)
{
    std::vector<const command*> current;
    int status = last_status;
    bool should_run = true;

    for (const auto& e : line->exprs) {

        if (e.type == EXPR_TYPE_COMMAND && e.cmd.has_value()) {
            current.push_back(&e.cmd.value());
        }
        else if (e.type == EXPR_TYPE_PIPE) {
            continue;
        }
        else if (e.type == EXPR_TYPE_AND ||
                 e.type == EXPR_TYPE_OR)
        {
            if (should_run) {
                status = execute_pipeline(current, line, status, should_exit);
                if (should_exit)
                    return status;
            }

            current.clear();

            if (e.type == EXPR_TYPE_AND)
                should_run = (status == 0);
            else
                should_run = (status != 0);
        }
    }

    if (should_run) {
        status = execute_pipeline(current, line, status, should_exit);
    }

    return status;
}

////////// REAP ZOMBIES //////////

static void reap_zombies()
{
    while (waitpid(-1, nullptr, WNOHANG) > 0) {
    }
}

////////// MAIN //////////

int main()
{
    struct parser* p = parser_new();
    int last_status = 0;

    while (true) {

        reap_zombies();

        if (isatty(STDIN_FILENO)) {
            printf("> ");
            fflush(stdout);
        }

        char buffer[4096];
        if (!fgets(buffer, sizeof(buffer), stdin))
            break;

        parser_feed(p, buffer, strlen(buffer));

        command_line* line = nullptr;
        parser_error err = parser_pop_next(p, &line);

        if (err != PARSER_ERR_NONE) {
            fprintf(stderr, "Parse error\n");
            continue;
        }

        if (line) {
            bool should_exit = false;
            last_status = execute_command_line(line, last_status, should_exit);
            delete line;

            if (should_exit) {
                parser_delete(p);
                return last_status;
            }
        }

        reap_zombies();
    }

    parser_delete(p);
    return last_status;
}
