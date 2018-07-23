-module(riak_repl2_object_filter).
-behaviour(gen_server).

%% API
-export([
    start_link/0,
    enable/0,
    disable/0,
    check_config/1,
    load_config/1,
    print_config/0,
    supported_filter_types/1,
    supported_match_types/1,
    get_versioned_config/1,
    get_versioned_config/2,
    create_config_for_remote_cluster/2,
    get_config/0,
    get_status/0,
    get_version/0,
    allowed_remotes/2,
    filter/2
]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).
-define(SUPPORTED_MATCH_TYPES(Version), supported_match_types(Version)).
-define(SUPPORTED_FILTER_TYPES(Version), supported_filter_types(Version)).
-define(STATUS, app_helper:get_env(riak_repl, object_filtering_status, disabled)).
-define(CONFIG, app_helper:get_env(riak_repl, object_filtering_config, [])).
-define(VERSION, app_helper:get_env(riak_repl, object_filtering_version, 0)).
-define(CLUSTERNAME, app_helper:get_env(riak_repl, clustername, "undefined")).
-define(CURRENT_VERSION, 1.0).

-record(state, {}).

%%%===================================================================
%%% Macro Helper Functions
%%%===================================================================
%%supported_match_types(1.1) ->
%%    [bucket, metadata, key];
supported_match_types(1.0) ->
    [bucket, metadata];
supported_match_types(_) ->
    [].

%%supported_filter_types(1.1) ->
%%    supported_filter_types(1.0);
supported_filter_types(1.0) ->
    [blacklist, whitelist];
supported_filter_types(_) ->
    [].

%%%===================================================================
%%% API (Function Callbacks)
%%%===================================================================
get_config() ->
    get_versioned_config(?VERSION).
get_status()->
    ?STATUS.
get_version() ->
    ?VERSION.
get_versioned_config(Version) ->
    get_versioned_config(?CONFIG, Version).
get_versioned_config(Config, Version) ->
    case Version >= ?VERSION of
        true ->
            Config;
        false ->
            downgrade_config(Config, Version)
    end.

create_config_for_remote_cluster(ClusterName, AgreedVersion) ->
    Config = get_versioned_config(AgreedVersion),
    invert_config(ClusterName, Config).

allowed_remotes(Remotes, {{whitelist, Whitelist}, {blacklist, Blacklist}}) ->
    WhitelistedRemotes = sets:to_list(sets:intersection(sets:from_list(Remotes), sets:from_list(Whitelist))),
    WhitelistedRemotes -- Blacklist.

filter({fullsync, disabled, _, _, _}, _) ->
    false;
filter({fullsync, enabled, 0, _, _}, _) ->
    false;
filter({fullsync, enabled, Version, Config, RemoteName}, Object) ->
    Bucket = riak_object:bucket(Object),
    Metadatas = riak_object:get_metadatas(Object),
    FilteredRemotes = filter_helper(Version, Config, {Bucket, Metadatas}, {{whitelist, []}, {blacklist, []}}),
    AllowedRemotes = allowed_remotes([RemoteName], FilteredRemotes),
    lists:member(RemoteName, AllowedRemotes).



%%%===================================================================
%%% API (Function Callbacks) Helper Functions
%%%===================================================================
downgrade_config(Config, Version) ->
    downgrade_config_helper(Config, Version, []).
downgrade_config_helper([], _, NewConfig) ->
    NewConfig;
downgrade_config_helper([ Rule = {{MatchType, _MatchValue}, {FilterType, _RemoteNodes}} | Rest], Version, NewConfig) ->
    case {lists:member(MatchType, ?SUPPORTED_MATCH_TYPES(Version)), lists:member(FilterType, ?SUPPORTED_FILTER_TYPES(Version))} of
        {true, true} ->
            downgrade_config_helper(Rest, Version, NewConfig++[Rule]);
        _ ->
            downgrade_config_helper(Rest, Version, NewConfig)
    end.

invert_config(ClusterName, Config) ->
    invert_config_helper(ClusterName, Config, []).
invert_config_helper(_, [], NewConfig) ->
    NewConfig;
invert_config_helper(ClusterName, [{{MatchType, MatchValue}, {FilterType, RemoteNodes}} | Rest], NewConfig) ->
    case FilterType of
        whitelist ->
            case lists:member(ClusterName, RemoteNodes) of
                true ->
                    invert_config_helper(ClusterName, Rest, NewConfig);
                false ->
                    Rule = [{{MatchType, MatchValue}, {blacklist, ?CLUSTERNAME}}],
                    invert_config_helper(ClusterName, Rest, NewConfig++Rule)
            end;
        blacklist ->
            case lists:member(ClusterName, RemoteNodes) of
                true ->
                    Rule = [{{MatchType, MatchValue}, {blacklist, ?CLUSTERNAME}}],
                    invert_config_helper(ClusterName, Rest, NewConfig++Rule);
                false ->
                    invert_config_helper(ClusterName, Rest, NewConfig)
            end
    end.

filter_helper(0, _Config, {_Bucket, _Metadatas}, FilteredRemotes) ->
    FilteredRemotes;
filter_helper(_V, [], _D, FilteredRemotes) ->
    FilteredRemotes;
filter_helper(Version, [{{MatchType, MatchValue}, {FilterType, RemoteNodes}} | RestOfRules], Data, FilteredRemotes) ->
    case does_data_match_rule(MatchType, MatchValue, Data) of
        true ->
            NewFilteredRemotes = add_filtered_remote(FilterType, RemoteNodes, FilteredRemotes),
            filter_helper(Version, RestOfRules, Data, NewFilteredRemotes);
        false ->
            filter_helper(Version, RestOfRules, Data, FilteredRemotes)
    end.



does_data_match_rule(bucket, MatchBucket, {Bucket, _Metadatas}) ->
    MatchBucket == Bucket;
does_data_match_rule(metadata, {MatchKey, MatchValue}, {_Bucket, Metadatas}) ->
    match_metadata(MatchKey, MatchValue, Metadatas).

match_metadata(_, _, []) ->
    false;
match_metadata(MatchKey, MatchValue, [Dict | Rest]) ->
    case dict:find(MatchKey, Dict) of
        {ok, MatchValue} ->
            true;
        _ ->
            match_metadata(MatchKey, MatchValue, Rest)
    end.


add_filtered_remote(whitelist, RemoteNodes, {{whitelist, W}, {blacklist, B}}) ->
    {{whitelist, W++RemoteNodes}, {blacklist, B}};
add_filtered_remote(blacklist, RemoteNodes, {{whitelist, W}, {blacklist, B}}) ->
    {{whitelist, W}, {blacklist, B++RemoteNodes}}.





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
%%% gen_server callbacks
%%%===================================================================
init([]) ->
    {Status, Config} = riak_repl_ring:get_object_filtering_data(),
    application:set_env(riak_repl, object_filtering_status, Status),
    application:set_env(riak_repl, object_filtering_config, Config),
    Version = riak_core_capability:get({riak_repl, object_filtering_version}, 0),
    application:set_env(riak_repl, object_filtering_version, Version),
    application:set_env(riak_repl, clustername, riak_core_connection:symbolic_clustername()),
    case Version == ?CURRENT_VERSION of
        false ->
            erlang:send_after(5000, self(), poll_core_capability);
        true ->
            ok
    end,
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

handle_info(poll_core_capability, State) ->
    Version = riak_core_capability:get({riak_repl, object_filtering_version}, 0),
    case Version == ?CURRENT_VERSION of
        true ->
            erlang:send_after(5000, self(), poll_core_capability);
        false ->
            application:set_env(riak_repl, object_filtering_version, Version)
    end,
    {noreply, State};
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
                            SortedConfig = sort_config(FilteringRules),
                            riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_object_filtering_config/2, SortedConfig),
                            application:set_env(riak_repl, object_filtering_config, SortedConfig),
                            ok
                    end;
                Error2 ->
                    Error2
            end;
        Error1 ->
            Error1
    end.

object_filtering_config() ->
    {print_config, {?VERSION, ?STATUS, ?CONFIG}}.


check_filtering_rules([]) -> {error, {no_rules, ?VERSION}};
check_filtering_rules(FilteringRules) -> check_filtering_rules_helper(FilteringRules, 1).
check_filtering_rules_helper([], _) -> ok;
check_filtering_rules_helper([{{MatchType, MatchValue}, {FilterType, _RemoteNodes}} | RestOfRules], N) ->
    case duplicate_rule({MatchType, MatchValue}, RestOfRules) of
        false ->
            case lists:member(MatchType, ?SUPPORTED_MATCH_TYPES(?VERSION)) of
                true ->
                    case lists:member(FilterType, ?SUPPORTED_FILTER_TYPES(?VERSION)) of
                        true ->
                            check_filtering_rules_helper(RestOfRules, N+1);
                        false ->
                            {error, {filter_type, ?VERSION, N, FilterType, ?SUPPORTED_FILTER_TYPES(?VERSION)}}
                    end;
                false ->
                    {error, {match_type, ?VERSION, N, MatchType, ?SUPPORTED_MATCH_TYPES(?VERSION)}}
            end;
        true ->
            {error, {duplicate_rule, ?VERSION, N, MatchType, MatchValue}}
    end.


duplicate_rule(Rule, RestOfRules) ->
    case lists:keyfind(Rule, 1, RestOfRules) of
        false ->
            false;
        _ ->
            true
    end.

sort_config(Config) ->
    sort_config_helper(Config, []).
sort_config_helper([], Sorted) ->
    Sorted;
sort_config_helper([Rule = {_, {blacklist, _}} | Rest], Sorted) ->
    sort_config_helper(Rest, Sorted++[Rule]);
sort_config_helper([Rule | Rest], Sorted) ->
    sort_config_helper(Rest, [Rule]++Sorted).







