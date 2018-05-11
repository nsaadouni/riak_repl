-module(riak_repl2_rtsource_conn_data_mgr).
-author("nordine saadouni").

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    set_leader/2,

    delete/2, delete/3, delete/5,
    read/1, read/2, read/3,
    write/4, write/5
]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).
-define(PROXY_CALL_TIMEOUT, 30 * 1000).

-record(state, {

    leader_node,
    is_leader,
    connections,
    active_nodes,
    polling_interval,
    restoration

}).

%%%===================================================================
%%% API
%%%===================================================================
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

set_leader(LeaderNode, _LeaderPid) ->
    gen_server:cast(?SERVER, {set_leader_node, LeaderNode}).

read(realtime_connections, Remote) ->
    gen_server:call(?SERVER, {read_realtime_connections, Remote}).

read(realtime_connections, Remote, Node) ->
    gen_server:call(?SERVER, {read_realtime_connections, Remote, Node}).

read(active_nodes) ->
    gen_server:call(?SERVER, read_active_nodes).

write(realtime_connections, Remote, Node, IPPort, Primary) ->
    gen_server:cast(?SERVER, {write_realtime_connections, Remote, Node, IPPort, Primary}).

write(realtime_connections, Remote, Node, ConnectionList) ->
    gen_server:cast(?SERVER, {write_realtime_connections, Remote, Node, ConnectionList}).

delete(realtime_connections, Remote) ->
    gen_server:cast(?SERVER, {delete_realtime_connections, Remote}).

delete(realtime_connections, Remote, Node) ->
    gen_server:cast(?SERVER, {delete_realtime_connections, Remote, Node}).

delete(realtime_connections, Remote, Node, IPPort, Primary) ->
    gen_server:cast(?SERVER, {delete_realtime_connections, Remote, Node, IPPort, Primary}).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    %% try catch leader_node
    {Leader, IsLeader} =
        try riak_repl2_leader:leader_node() of
            LeaderNode ->
                case LeaderNode == node() of
                    true ->
                        EmptyDict = dict:new(),
                        case riak_repl_ring:get_realtime_connection_data() of
                            EmptyDict ->
                                {LeaderNode, true};
                            _ ->
                                %% request connection data from proxy nodes as this could be a restart and we have lost all data
                                AllNodes = riak_core_node_watcher:nodes(riak_kv),
                                [ gen_server:cast({riak_repl2_leader, Node}, re_notify) || Node <- AllNodes -- [node()]],
                                riak_repl2_leader:re_notify(),
                                {node(), true}
                        end;
                    false ->
                        {LeaderNode, false}
                end
        catch
            _Type:_Error ->
                {undefined, false}
        end,

    C = dict:new(),
    AN = [],
    lager:debug("conn_data_mgr started"),
    riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_realtime_connection_data/2, C),
    PollingIntervalSecs = app_helper:get_env(riak_repl, realtime_node_watcher_polling_interval, 10),
    PollingInterval = PollingIntervalSecs * 1000,
    lager:debug("node watcher polling interval: ~p", [PollingInterval]),
    erlang:send_after(PollingInterval, self(), poll_node_watcher),
    {ok, #state{is_leader = IsLeader, leader_node = Leader, connections = C, active_nodes = AN, polling_interval = PollingInterval, restoration = false}}.


%% -------------------------------------------------- Read ---------------------------------------------------------- %%

handle_call(Msg={read_realtime_connections, Remote}, _From, State=#state{connections = C, is_leader = L}) ->
    case L of
        true ->
            NodeDict = get_value(Remote, C, dictionary),
            {reply, NodeDict, State};
        false ->
            NoLeaderResult = no_leader,
            proxy_call(Msg, NoLeaderResult, State)
    end;

handle_call(Msg={read_realtime_connections, Remote, Node}, _From, State=#state{connections = C, is_leader = L}) ->
    case L of
        true ->
            NodeDict = get_value(Remote, C, dictionary),
            ConnsList = get_value(Node, NodeDict, list),
            {reply, ConnsList, State};
        false ->
            NoLeaderResult = no_leader,
            proxy_call(Msg, NoLeaderResult, State)
    end;

handle_call(Msg=read_active_nodes, _From, State=#state{active_nodes = AN, is_leader = L}) ->
    case L of
        true ->
            {reply, AN, State};
        false ->
            NoLeaderResult = no_leader,
            proxy_call(Msg, NoLeaderResult, State)
    end;

handle_call({proxy_cast_handle, CastMsg}, _From, State=#state{restoration = R}) ->
    case CastMsg of
        {restore_realtime_connections,_,_,_} ->
            gen_server:cast(?SERVER, CastMsg),
            {reply, ok, State};
        Msg ->
            case R of
                true ->
                    {reply, restoration_in_process, State};
                false ->
                    gen_server:cast(?SERVER, Msg),
                    {reply, ok, State}
            end
    end;

handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast({set_leader_node, LeaderNode}, State) ->
    lager:info("setting leader node as: ~p", [LeaderNode]),
    case node() of
        LeaderNode ->
            {noreply, become_leader(State, LeaderNode)};
        _ ->
            {noreply, become_proxy(State, LeaderNode)}
    end;

%% -------------------------------------------------- Delete -------------------------------------------------------- %%

handle_cast(Msg={delete_realtime_connections, Remote, Node, IPPort, Primary}, State=#state{connections = C, is_leader = L}) ->
    case L of
        true ->
            OldNodeDict = get_value(Remote, C, dictionary),
            OldConnsList = get_value(Node, OldNodeDict, list),
            NewConnsList = lists:delete({IPPort, Primary}, OldConnsList),
            NewNodeDict = dict:store(Node, NewConnsList, OldNodeDict),
            NewConnections = dict:store(Remote, NewNodeDict, C),

            % push to ring
            riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_realtime_connection_data/2, NewConnections),
            {noreply, State#state{connections = NewConnections}};

        false ->
            proxy_cast(Msg, State),
            {noreply, State}
    end;

handle_cast(Msg={delete_realtime_connections, Remote, Node}, State=#state{connections = C, is_leader = L}) ->
    case L of
        true ->
            OldNodeDict = get_value(Remote, C, dictionary),
            NewNodeDict = dict:erase(Node, OldNodeDict),
            NewConnections = dict:store(Remote, NewNodeDict, C),
            lager:debug("deleted realtime connection data ~p ~p", [Remote, Node]),

            % push to ring
            riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_realtime_connection_data/2, NewConnections),
            {noreply, State#state{connections = NewConnections}};

        false ->
            proxy_cast(Msg, State),
            {noreply, State}
    end;

handle_cast(Msg = {delete_realtime_connections, Remote}, State=#state{connections = C, is_leader = L}) ->
    case L of
        true ->
            NewConnections = dict:erase(Remote,C),

            % push to ring
            riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_realtime_connection_data/2, NewConnections),
            {noreply, State#state{connections = NewConnections}};

        false ->
            proxy_cast(Msg, State),
            {noreply, State}
    end;


%% -------------------------------------------------- Write -------------------------------------------------------- %%

handle_cast(Msg = {write_realtime_connections, Remote, Node, IPPort, Primary}, State=#state{connections = C}) ->
    case State#state.is_leader of
        true ->
            OldRemoteDict = get_value(Remote, C, dictionary),
            NewRemoteDict = dict:append(Node, {IPPort, Primary}, OldRemoteDict),
            NewConnections = dict:store(Remote, NewRemoteDict, C),

            % push onto ring
            riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_realtime_connection_data/2, NewConnections),
            {noreply, State#state{connections = NewConnections}};

        false ->
            proxy_cast(Msg, State),
            {noreply, State}
    end;

handle_cast(Msg = {write_realtime_connections, Remote, Node, ConnectionList}, State = #state{connections = C}) ->
    case State#state.is_leader of
        true ->
            lager:info("data_mgr is leader writing -> ~p node = ~p", [Msg, node()]),
            OldRemoteDict = get_value(Remote, C, dictionary),
            NewRemoteDict = dict:store(Node, ConnectionList, OldRemoteDict),
            NewConnections = dict:store(Remote, NewRemoteDict, C),

            % push onto ring
            riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_realtime_connection_data/2, NewConnections),
            {noreply, State#state{connections = NewConnections}};

        false ->
            lager:info("data_mgr is proxy sending to leader -> ~p
      node = ~p", [Msg, node()]),
            proxy_cast(Msg, State),
            {noreply, State}
    end;

handle_cast(Msg = {restore_realtime_connections, Remote, Node, ConnectionList}, State = #state{connections = C}) ->
    case State#state.is_leader of
        true ->
            lager:info("data_mgr is leader writing -> ~p node = ~p", [Msg, node()]),

            OldRemoteDict = get_value(Remote, C, dictionary),
            OldConns = get_value(Node, OldRemoteDict, list),
            NewConns = lists:usort(OldConns ++ ConnectionList),
            NewRemoteDict = dict:store(Node, NewConns, OldRemoteDict),
            NewConnections = dict:store(Remote, NewRemoteDict, C),

            % push onto ring
            riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_realtime_connection_data/2, NewConnections),
            {noreply, State#state{connections = NewConnections}};

        false ->
            lager:info("data_mgr is proxy sending to leader -> ~p
      node = ~p", [Msg, node()]),
            proxy_cast(Msg, State),
            {noreply, State}
    end;

handle_cast(_Request, State) ->
    {noreply, State}.


handle_info(restore_leader_data, State) ->
    RemoteRealtimeConnections = riak_repl_ring:get_realtime_connection_data(),
    ActiveNodes = riak_repl_ring:get_active_nodes(),
    {noreply, State#state{connections = RemoteRealtimeConnections, active_nodes = ActiveNodes}};

handle_info(reset_restoration_flag, State) ->
    {noreply, State#state{restoration = false}};

handle_info(poll_node_watcher, State=#state{active_nodes = OldActiveNodes, connections = C, polling_interval = PI}) when State#state.is_leader == true ->
    NewActiveNodes = riak_core_node_watcher:nodes(riak_kv),
    DownNodes = OldActiveNodes -- NewActiveNodes,
    UpNodes = NewActiveNodes -- OldActiveNodes,
    Connections = case {DownNodes, UpNodes} of
                      {[], []} ->
                          C;
                      {[], _Up} ->
                          riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_active_nodes/2, NewActiveNodes),
                          C;
                      {Down, _} ->
                          AllRemotes = dict:fetch_keys(C),
                          lager:debug("down nodes = ~p; known remotes = ~p; connections = ~p", [DownNodes, AllRemotes, C]),
                          NewC = remove_nodes_remotes(Down, AllRemotes, C),
                          lager:debug("down nodes, new connections dictionary ~p", [NewC]),
                          riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_active_nodes_and_realtime_connection_data/2, {NewC, NewActiveNodes}),
                          NewC
                  end,
    erlang:send_after(PI, self(), poll_node_watcher),
    {noreply, State#state{active_nodes = NewActiveNodes, connections = Connections}};
handle_info(poll_node_watcher, State=#state{polling_interval = PI}) ->
    erlang:send_after(PI, self(), poll_node_watcher),
    {noreply, State};

handle_info({cast_again, CastMsg}, State) ->
    gen_server:cast(?SERVER, CastMsg),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.


terminate(Reason, State) ->
    lager:info("riak_repl2_rtsource_conn_data_mgr termianting due to: ~p
            State: ~p", [Reason, State]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
remove_nodes_remotes(_Nodes, [], ConnetionsDict) ->
    ConnetionsDict;
remove_nodes_remotes(Nodes, [Remote|Remotes], ConnectionsDict) ->
    remove_nodes_remotes(Nodes, Remotes, remove_nodes(Nodes, Remote, ConnectionsDict)).
remove_nodes([], _Remote, ConnectionsDict) ->
    ConnectionsDict;
remove_nodes([Node|Nodes], Remote, ConnectionsDict) ->
    OldNodeDict = get_value(Remote, ConnectionsDict, dictionary),
    NewNodeDict = dict:erase(Node, OldNodeDict),
    remove_nodes(Nodes, Remote, dict:store(Remote, NewNodeDict, ConnectionsDict)).


become_leader(State, LeaderNode) when State#state.is_leader == false ->
    send_leader_data(),
    erlang:send_after(5000, self(), reset_restoration_flag),
    State#state{is_leader = true, leader_node = LeaderNode, restoration = true};%% active_nodes = ActiveNodes};
become_leader(State, LeaderNode) ->
    State#state{is_leader = true, leader_node = LeaderNode}.


become_proxy(State, LeaderNode) when State#state.is_leader == true ->
    send_leader_data(),
    State#state{is_leader = false, leader_node = LeaderNode, connections = dict:new(), restoration = false};
become_proxy(State, LeaderNode) ->
    send_leader_data(),
    State#state{is_leader = false, leader_node = LeaderNode}.

send_leader_data() ->
    ConnMgrsPids = riak_repl2_rtsource_conn_sup:enabled(),
    AllEndpoints =  [{Remote,ConnsList} || {Remote, ConnsList} <- lists:map(fun({R,Pid}) -> {R, dict:fetch_keys(riak_repl2_rtsource_conn_mgr:get_endpoints(Pid))} end, ConnMgrsPids)],
    [gen_server:cast(?SERVER, {restore_realtime_connections, Remote, node(), ConnectionList}) || {Remote, ConnectionList} <- AllEndpoints].



proxy_cast(CastMsg, _State = #state{leader_node=Leader}) when Leader == undefined ->
    erlang:send_after(5000, self(), {cast_again, CastMsg}),
    ok;
proxy_cast(CastMsg, _State = #state{leader_node=Leader}) ->
    try gen_server:call({?SERVER, Leader}, {proxy_cast_handle, CastMsg}, ?PROXY_CALL_TIMEOUT) of
        restoration_in_process ->
            erlang:send_after(5000, self(), {cast_again, CastMsg});
        ok -> ok
    catch
        exit:_Error ->
            erlang:send_after(5000, self(), {cast_again, CastMsg})
    end,
    ok.

proxy_call(_Call, NoLeaderResult, State = #state{leader_node=Leader}) when Leader == undefined ->
    lager:debug("data_mgr no leader call: ~p", [_Call]),
    {reply, NoLeaderResult, State};
proxy_call(Call, NoLeaderResult, State = #state{leader_node=Leader}) ->
    Reply = try gen_server:call({?SERVER, Leader}, Call, ?PROXY_CALL_TIMEOUT) of
                R -> R
            catch
                exit:{noproc, _} ->
                    NoLeaderResult;
                exit:{{nodedown, _}, _} ->
                    NoLeaderResult
            end,
    {reply, Reply, State}.



get_value(Key, Dictionary, Type) ->
    case dict:find(Key, Dictionary) of
        {ok, X} ->
            X;
        error ->
            case Type of
                dictionary ->
                    dict:new();
                list ->
                    []
            end
    end.