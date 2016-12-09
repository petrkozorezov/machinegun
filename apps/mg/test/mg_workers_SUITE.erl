%%%
%%% Юнит тесты для воркеров.
%%% Задача — проверить корректность работы части отвечающей за автоматическое поднятие и выгрузку воркеров для машин.
%%%
%%% TODO:
%%%  - проверить выгрузку
%%%  - проверить ограничение очереди
%%%  -
%%%
-module(mg_workers_SUITE).
-include_lib("common_test/include/ct.hrl").

%% tests descriptions
-export([all             /0]).
-export([init_per_suite  /1]).
-export([end_per_suite   /1]).

%% tests
-export([base_test       /1]).
-export([load_fail_test  /1]).
-export([load_error_test /1]).
-export([call_fail_test  /1]).
-export([unload_fail_test/1]).
-export([stress_test     /1]).

%% mg_worker
-behaviour(mg_worker).
-export([handle_load/2, handle_call/2, handle_unload/1]).

%%
%% tests descriptions
%%
-type group_name() :: atom().
-type test_name () :: atom().
-type config    () :: [{atom(), _}].

-spec all() ->
    [test_name() | {group, group_name()}].
all() ->
    [
       base_test,
       load_fail_test,
       load_error_test,
       call_fail_test,
       unload_fail_test,
       stress_test
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({mg_machine_event_sink, '_', '_'}, x),
    Apps = genlib_app:start_application(mg),
    [{apps, Apps} | C].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    [application:stop(App) || App <- proplists:get_value(apps, C)].

%%
%% base group tests
%%
-define(UNLOAD_TIMEOUT, 10).
-spec base_test(config()) ->
    _.
base_test(_C) ->
    % чтобы увидеть падение воркера линкуемся к нему
    Options = workers_options(?UNLOAD_TIMEOUT, #{link_pid=>erlang:self()}),
    Pid     = start_workers(Options),
    hello   = mg_workers_manager:call(Options, 42, hello),
    ok      = wait_machines_unload(?UNLOAD_TIMEOUT),
    ok      = stop_workers(Pid).

-spec load_fail_test(config()) ->
    _.
load_fail_test(_C) ->
    % тут процесс специально падает, поэтому линк не нужен
    Options = workers_options(?UNLOAD_TIMEOUT, #{fail_on=>load}),
    Pid     = start_workers(Options),
    {error, {unexpected_exit, _}} =
        mg_workers_manager:call(Options, 42, hello),
    ok      = wait_machines_unload(?UNLOAD_TIMEOUT),
    ok      = stop_workers(Pid).

-spec load_error_test(config()) ->
    _.
load_error_test(_C) ->
    % чтобы увидеть падение воркера линкуемся к нему
    Options = workers_options(?UNLOAD_TIMEOUT, #{load_error=>test_error, link_pid=>erlang:self()}),
    Pid     = start_workers(Options),
    {error, test_error} = mg_workers_manager:call(Options, 42, hello),
    ok      = wait_machines_unload(?UNLOAD_TIMEOUT),
    ok      = stop_workers(Pid).

-spec call_fail_test(config()) ->
    _.
call_fail_test(_C) ->
    % тут процесс специально падает, поэтому линк не нужен
    Options = workers_options(?UNLOAD_TIMEOUT, #{fail_on=>call}),
    Pid     = start_workers(Options),
    {error, {unexpected_exit, _}} =
        mg_workers_manager:call(Options, 43, hello),
    ok      = wait_machines_unload(?UNLOAD_TIMEOUT),
    ok      = stop_workers(Pid).

-spec unload_fail_test(config()) ->
    _.
unload_fail_test(_C) ->
    % падение при unload'е мы не замечаем :(
    Options = workers_options(?UNLOAD_TIMEOUT, #{fail_on=>unload}),
    Pid     = start_workers(Options),
    hello   = mg_workers_manager:call(Options, 42, hello),
    ok      = wait_machines_unload(?UNLOAD_TIMEOUT),
    ok      = stop_workers(Pid).

-spec stress_test(config()) ->
    _.
stress_test(_C) ->
    TestTimeout        = 5 * 1000,
    WorkersCount       = 50,
    TestProcessesCount = 1000,
    UnloadTimeout      = 2,

    Options = workers_options(UnloadTimeout, #{link_pid=>erlang:self()}),
    WorkersPid = start_workers(Options),

    TestProcesses = [stress_test_start_process(Options, WorkersCount) || _ <- lists:seq(1, TestProcessesCount)],
    ok = timer:sleep(TestTimeout),

    ok = stop_wait_all(TestProcesses, shutdown, 1000),
    ok = wait_machines_unload(UnloadTimeout),
    ok = stop_workers(WorkersPid).

-spec stress_test_start_process(mg_workers_manager:options(), pos_integer()) ->
    pid().
stress_test_start_process(Options, WorkersCount) ->
    erlang:spawn_link(fun() -> stress_test_process(Options, WorkersCount) end).

-spec stress_test_process(mg_workers_manager:options(), pos_integer()) ->
    no_return().
stress_test_process(Options, WorkersCount) ->
    ok = stress_test_do_test_call(Options, WorkersCount),
    stress_test_process(Options, WorkersCount).

-spec stress_test_do_test_call(mg_workers_manager:options(), pos_integer()) ->
    ok.
stress_test_do_test_call(Options, WorkersCount) ->
    ID = rand:uniform(WorkersCount),
    % проверим, что отвечают действительно на наш запрос
    Call = {hello, erlang:make_ref()},
    Call = mg_workers_manager:call(Options, ID, Call),
    ok.

-spec workers_options(non_neg_integer(), worker_params()) ->
    mg_workers_manager:options().
workers_options(UnloadTimeout, WorkerParams) ->
    #{
        name           => base_test_workers,
        worker_options => #{
            worker            => {?MODULE, WorkerParams},
            hibernate_timeout => UnloadTimeout div 2,
            unload_timeout    => UnloadTimeout
        }
    }.

%%
%% worker callbacks
%%
%% Реализуется простая логика с поднятием, принятием запроса и выгрузкой.
%%
-type worker_stage() :: load | call | unload.
-type worker_params() :: #{
    link_pid   => pid(),
    load_error => term(),
    fail_on    => worker_stage()
}.
-type worker_state() :: worker_params().

-spec handle_load(_ID, worker_params()) ->
    {ok, worker_state()} | {error, _}.
handle_load(_, #{load_error := Reason}) ->
    {error, Reason};
handle_load(_, Params) ->
    ok = try_link(Params),
    ok = try_exit(load, Params),
    {ok, Params}.

-spec handle_call(_Call, worker_state()) ->
    {_Resp, worker_state()}.
handle_call(Call, State) ->
    ok = try_exit(call, State),
    {Call, State}.

-spec handle_unload(worker_state()) ->
    ok.
handle_unload(State) ->
    ok = try_exit(unload, State),
    ok = try_unlink(State).

-spec try_exit(worker_stage(), worker_params()) ->
    ok.
try_exit(CurrentStage, #{fail_on := FailOnStage}) when CurrentStage =:= FailOnStage ->
    exit(fail);
try_exit(_Stage, #{}) ->
    ok.

-spec try_link(worker_params()) ->
    ok.
try_link(#{link_pid:=Pid}) ->
    true = erlang:link(Pid), ok;
try_link(#{}) ->
    ok.

-spec try_unlink(worker_params()) ->
    ok.
try_unlink(#{link_pid:=Pid}) ->
    true = erlang:unlink(Pid), ok;
try_unlink(#{}) ->
    ok.

%%
%% utils
%%
-spec start_workers(_Options) ->
    pid().
start_workers(Options) ->
    {ok, Pid} =
        mg_utils_supervisor_wrapper:start_link(
            #{strategy => one_for_all},
            [mg_workers_manager:child_spec(workers, Options)]
        ),
    Pid.

-spec stop_workers(pid()) ->
    ok.
stop_workers(Pid) ->
    true = erlang:unlink(Pid),
    true = erlang:exit(Pid, kill),
    ok.

-spec stop_wait_all([pid()], _Reason, timeout()) ->
    ok.
stop_wait_all(Pids, Reason, Timeout) ->
    lists:foreach(
        fun(Pid) ->
            case stop_wait(Pid, Reason, Timeout) of
                ok      -> ok;
                timeout -> exit(stop_timeout)
            end
        end,
        Pids
    ).

-spec stop_wait(pid(), _Reason, timeout()) ->
    ok | timeout.
stop_wait(Pid, Reason, Timeout) ->
    OldTrap = process_flag(trap_exit, true),
    erlang:exit(Pid, Reason),
    R =
        receive
            {'EXIT', Pid, Reason} -> ok
        after
            Timeout -> timeout
        end,
    process_flag(trap_exit, OldTrap),
    R.

-spec wait_machines_unload(pos_integer()) ->
    ok.
wait_machines_unload(UnloadTimeout) ->
    ok = timer:sleep(UnloadTimeout * 2).