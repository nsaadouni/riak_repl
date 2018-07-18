-module(riak_repl2_object_filter).
-behaviour(gen_server).

%% API
-export([
    start_link/0,
    enable/0,
    disable/0,
    check_config/1,
    load_config/1,
    print_config/0
]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).
-define(SUPPORTED_MATCH_TYPES, [bucket, metadata]).
-define(SUPPORTED_FILTER_TYPES, [whitelist, blacklist]).


-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).
enable()->
    gen_server:call(?SERVER, enable).
disable()->
    gen_server:call(?SERVER, disable).
check_config(ConfigFilePath) ->
    gen_server:call(?SERVER, {check_config, ConfigFilePath}).
load_config(ConfigFilePath) ->
    gen_server:call(?SERVER, {load_config, ConfigFilePath}).
print_config() ->
    gen_server:call(?SERVER, print_config).

%%%===================================================================
%%% API (Function Callbacks)
%%%===================================================================


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([]) ->
    {Status, Config} = riak_repl_ring:get_object_filtering_data(),
    application:set_env(riak_repl, object_filtering_status, Status),
    application:set_env(riak_repl, object_filtering_config, Config),
    {ok, #state{}}.

handle_call(Request, _From, State) ->
    Response = case Request of
                   enable ->
                       object_filtering_enable();
                   disable ->
                       object_filtering_disable();
                   {check_config, Path} ->
                       object_filtering_config_file(check, Path);
                   {load_config, Path} ->
                       object_filtering_config_file(load, Path);
                   print_config ->
                       object_filtering_config();
                   _ ->
                       error
               end,
    {reply, Response, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
object_filtering_disable() ->
    riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_object_filtering_status/2, disabled),
    application:set_env(riak_repl, object_filtering_status, disabled),
    ok.

object_filtering_enable() ->
    riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_object_filtering_status/2, enabled),
    application:set_env(riak_repl, object_filtering_status, enabled),
    ok.

object_filtering_config_file(Action, Path) ->
    case file:consult(Path) of
        {ok, FilteringRules} ->
            case check_filtering_rules(FilteringRules) of
                ok ->
                    case Action of
                        check -> ok;
                        load ->
                            riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_object_filtering_config/2, FilteringRules),
                            application:set_env(riak_repl, object_filtering_config, FilteringRules),
                            ok
                    end;
                Error2 ->
                    Error2
            end;
        Error1 ->
            Error1
    end.

object_filtering_config() ->
    ok.


check_filtering_rules([]) -> lists:flatten(io_lib:format("Error: No rules are present in the file", []));
check_filtering_rules(FilteringRules) -> check_filtering_rules_helper(FilteringRules, 1).
check_filtering_rules_helper([], _) -> ok;
check_filtering_rules_helper([{{MatchType, _MatchValue}, {FilterType, _RemoteNodes}} | RestOfRules], N) ->
    case lists:member(MatchType, ?SUPPORTED_MATCH_TYPES) of
        true ->
            case lists:member(FilterType, ?SUPPORTED_FILTER_TYPES) of
                true ->
                    check_filtering_rules_helper(RestOfRules, N+1);
                false ->
                    FilterTypeError = lists:flatten(io_lib:format("Error: [Rule ~p] Filter Type: ~p not supported, only ~p are supported", [N, FilterType, ?SUPPORTED_FILTER_TYPES])),
                    FilterTypeError
            end;
        false ->
            MatchTypeError = lists:flatten(io_lib:format("Error: [Rule ~p] Match Type: ~p not supported, only ~p are supported", [N, MatchType, ?SUPPORTED_MATCH_TYPES])),
            MatchTypeError
    end.




