%%%
%%% Кеш для стораджа.
%%% Не умеет инвалидировать записи, поэтому можно применять только для иммутабельных данных.
%%% Сохраняет только данные по ключу (не кеширует поиски, т.к. тогда нужно инвалидировать кеш).
%%%
-module(mg_storage_cache).

%% mg_storage callbacks
-behaviour(mg_storage).
-export_type([options/0]).
-export([child_spec/3, do_request/3]).
-export([start_link/2]).

-type options() :: #{
    storage := mg_storage:options(),
    cache   := cache_options()
}.

-type cache_options() :: #{
    type       => set | ordered_set,
    policy     => lru | mru,
    memory     => integer(),
    size       => integer(),
    n          => integer(),
    ttl        => integer(), %% seconds
    check      => integer(),
    stats      => function() | {module(), atom()},
    heir       => atom() | pid()
}.

%%
%% mg_storage callbacks
%%
-spec child_spec(options(), atom(), mg_utils:gen_reg_name()) ->
    supervisor:child_spec().
child_spec(Options, ChildID, RegName) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options, RegName]},
        restart  => permanent,
        type     => supervisor
    }.

-spec start_link(options(), mg_utils:gen_reg_name()) ->
    mg_utils:gen_start_ret().
start_link(Options = #{storage := Storage}, RegName) ->
    mg_utils_supervisor_wrapper:start_link(
        RegName,
        #{strategy => rest_for_one},
        [
            cache_child_spec(Options, cache, cache_reg_name(RegName)),
            fix_sub_storage_restart_type(mg_storage:child_spec(Storage, storage, sub_storage_reg_name(RegName)))
        ]
    ).

-spec do_request(options(), mg_utils:gen_ref(), mg_storage:request()) ->
    mg_storage:response() | no_return().
do_request(Options, Ref, Req = {get, Key}) ->
    case cache:get(cache_ref(Ref), Key) of
        undefined ->
            R = do_sub_request(Options, Ref, Req),
            ok = cache:put(cache_ref(Options), Key, {ok, R}),
            R;
        {ok, R} ->
            R
    end;
do_request(Options, Ref, Req) ->
    do_sub_request(Options, Ref, Req).

-spec do_sub_request(options(), mg_utils:gen_ref(), mg_storage:request()) ->
    mg_storage:response() | no_return().
do_sub_request(#{storage := Storage}, Ref, Req) ->
    mg_storage:child_spec(Storage, sub_storage_ref(Ref), Req).

%%

-spec cache_reg_name(mg_utils:gen_reg_name()) ->
    mg_utils:gen_reg_name().
cache_reg_name(RegName) ->
    gproc_register({?MODULE, RegName, cache}).

-spec cache_ref(mg_utils:gen_ref()) ->
    mg_utils:gen_ref().
cache_ref(Ref) ->
    gproc_register({?MODULE, check_ref(Ref), cache}).

-spec sub_storage_reg_name(mg_utils:gen_reg_name()) ->
    mg_utils:gen_reg_name().
sub_storage_reg_name(RegName) ->
    gproc_register({?MODULE, RegName, storage}).

-spec sub_storage_ref(mg_utils:gen_ref()) ->
    mg_utils:gen_ref().
sub_storage_ref(Ref) ->
    gproc_register({?MODULE, check_ref(Ref), storage}).

-spec gproc_register(_Key) ->
    {via, gproc, gproc:key()}.
gproc_register(Key) ->
    {via, gproc, {n, l, Key}}.

-spec check_ref(mg_utils:gen_ref()) ->
    mg_utils:gen_ref().
check_ref(Ref) when is_pid(Ref) ->
    % если сюда передать pid, то ничего работать не будет :)
    exit(unsupported);
check_ref(Ref) ->
    Ref.

%%

-spec cache_child_spec(atom(), options(), mg_utils:gen_reg_name()) ->
    supervisor:child_spec().
cache_child_spec(ChildID, Options, RegName) ->
    #{
        id       => ChildID,
        start    => {cache, start_link, [RegName, cache_options(Options)]},
        restart  => permanent,
        type     => supervisor
    }.

-spec cache_options(options()) ->
    cache:options().
cache_options(#{cache := Options}) ->
    maps:to_list(Options).

-spec fix_sub_storage_restart_type(supervisor:child_spec()) ->
    supervisor:child_spec().
fix_sub_storage_restart_type(Spec = #{}) ->
    Spec#{restart => temporary}.
