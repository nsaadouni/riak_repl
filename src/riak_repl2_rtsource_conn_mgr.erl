-module(riak_repl2_rtsource_conn_mgr).
-author("nordine saadouni").
-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-export([
  connected/7,
  connect_failed/3,
  maybe_rebalance_delayed/1,
  connection_closed/3,
  should_rebalance/3,
  stop/1,
  get_all_status/1,
  get_all_status/2,
  get_source_and_sink_nodes/1
]).

-define(SERVER, ?MODULE).
-define(KILL_TIME, 10*1000).

-define(CLIENT_SPEC, {{realtime,[{3,0}, {2,0}, {1,5}]},
  {?TCP_OPTIONS, ?SERVER, self()}}).

-define(TCP_OPTIONS,  [{keepalive, true},
  {nodelay, true},
  {packet, 0},
  {active, false}]).

-record(state, {
  remote, % remote sink cluster name
  connection_ref, % reference handed out by connection manager
  rtsource_conn_sup, % The module name of the supervisor to start the child when we get a connection passed to us
  rb_timeout_tref, % Rebalance timeout timer reference
  rebalance_delay,
  source_nodes,
  sink_nodes,
  remove_endpoint,
  endpoints
}).

%%%===================================================================
%%% API
%%%===================================================================


start_link([Remote]) ->
  gen_server:start_link(?MODULE, [Remote], []).

connected(Socket, Transport, IPPort, Proto, RTSourceConnMgrPid, _Props, Primary) ->
  Transport:controlling_process(Socket, RTSourceConnMgrPid),
  gen_server:call(RTSourceConnMgrPid, {connected, Socket, Transport, IPPort, Proto, _Props, Primary}).

connect_failed(_ClientProto, Reason, RTSourceConnMgrPid) ->
  gen_server:cast(RTSourceConnMgrPid, {connect_failed, self(), Reason}).

maybe_rebalance_delayed(Pid) ->
  gen_server:cast(Pid, rebalance_delayed).

stop(Pid) ->
  gen_server:call(Pid, stop).

connection_closed(Pid, Addr, Primary) ->
  gen_server:call(Pid, {connection_closed, Addr, Primary}).

get_all_status(Pid) ->
  get_all_status(Pid, infinity).
get_all_status(Pid, Timeout) ->
  gen_server:call(Pid, all_status, Timeout).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


init([Remote]) ->
  lager:debug("connecting to remote ~p", [Remote]),
  case riak_core_connection_mgr:connect({rt_repl, Remote}, ?CLIENT_SPEC, multi_connection) of
    {ok, Ref} ->
      _ = riak_repl2_rtq:register(Remote), % re-register to reset stale deliverfun
      lager:debug("connection ref ~p", [Ref]),
      S = riak_repl2_rtsource_conn_2_sup:make_module_name(Remote),
      E = orddict:new(),

      MaxDelaySecs = app_helper:get_env(riak_repl, realtime_connection_rebalance_max_delay_secs, 60),
      M = round(MaxDelaySecs * crypto:rand_uniform(0, 1000)),

      {SourceNodes, SinkNodes} = get_source_and_sink_nodes(Remote),

      lager:debug("conn_mgr node source: ~p", [SourceNodes]),
      lager:debug("conn_mgr node sink: ~p", [SinkNodes]),

      {ok, #state{remote = Remote, connection_ref = Ref, rtsource_conn_sup = S, endpoints = E, rebalance_delay = M,
        source_nodes = SourceNodes, sink_nodes = SinkNodes}};
    {error, Reason}->
      lager:warning("Error connecting to remote"),
      {stop, Reason}
  end.

%%%=====================================================================================================================
handle_call({connected, Socket, Transport, IPPort, Proto, _Props, Primary}, _From,
    State = #state{rtsource_conn_sup = S, remote = Remote, endpoints = E}) ->

  lager:debug("rtsource_conn_mgr connection recieved ~p", [{IPPort, Primary}]),

  case start_rtsource_conn(Remote, S) of
    {ok, RtSourcePid} ->
      lager:debug("we have added the connection"),
      case riak_repl2_rtsource_conn:connected(Socket, Transport, IPPort, Proto, RtSourcePid, _Props, Primary) of
        ok ->

          % check remove_endpoint
          NewState = case State#state.remove_endpoint of
                       undefined ->
                        State;
                      RC ->
                        E2 = remove_connections(RC, E),
                        State#state{endpoints = E2, remove_endpoint = undefined}
                     end,

          %% Save {EndPoint, Pid}; Pid will come from the supervisor starting a child
          NewEndpoints = orddict:store({IPPort, Primary}, RtSourcePid, NewState#state.endpoints),

          % save to ring
          lager:debug("rtsource_conn_mgr send connection data to data mgr"),
          riak_repl2_rtsource_conn_data_mgr:write(realtime_connections, Remote, node(), IPPort, Primary),

          {reply, ok, NewState#state{endpoints = NewEndpoints}};

        Error ->
          lager:debug("rtsouce_conn failed to recieve connection"),
          % need to ask the connection manager to re-try this address and create new port
          % core_connection_mgr:connect(use_only_addr)
          {reply, Error, State}
      end;
    ER ->
      {reply, ER, State}
  end;

handle_call(all_status, _From, State=#state{endpoints = E}) ->
  AllKeys = orddict:fetch_keys(E),
  {reply, collect_status_data(AllKeys, [], E), State};

handle_call({connection_closed, Addr, Primary}, _From, State=#state{endpoints = E, remote = R}) ->
  NewEndpoints = orddict:erase({Addr,Primary}, E),
  riak_repl2_rtsource_conn_data_mgr:delete(realtime_connections, R, node(), Addr, Primary),
  {reply, ok, State#state{endpoints = NewEndpoints}};

handle_call(stop, _From, State) ->
  lager:debug("stop rtsource_conn_mgr"),
  {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

%%%=====================================================================================================================

%% Connection manager failed to make connection
handle_cast({connect_failed, _HelperPid, Reason}, State = #state{remote = Remote}) ->
  lager:warning("Realtime replication connection to site ~p failed - ~p\n", [Remote, Reason]),
  {stop, normal, State};

handle_cast({kill_rtsource_conn, Pid}, State) ->
  lager:debug("rtsource_chain_kill rtsource_conn killed"),
  catch riak_repl2_rtsource_conn:stop(Pid),
  {noreply, State};


handle_cast(rebalance_delayed, State) ->
  {noreply, maybe_rebalance(State, delayed)};

handle_cast(_Request, State) ->
  {noreply, State}.

%%%=====================================================================================================================

handle_info(rebalance_now, State) ->
  {noreply, maybe_rebalance(State#state{rb_timeout_tref = undefined}, now)};

handle_info({kill_rtsource_conn, RtSourceConnPid}, State) ->
  riak_repl2_rtsource_conn:stop(RtSourceConnPid),
  {noreply, State};


handle_info(_Info, State) ->
  {noreply, State}.

%%%=====================================================================================================================

terminate(_Reason, _State=#state{remote = Remote, endpoints = E}) ->
  lager:debug("rtrsource_conn_mgr terminating"),
  riak_core_connection_mgr:disconnect({rt_repl, Remote}),
  [catch riak_repl2_rtsource_conn:stop(Pid) || {_,{Pid,_}} <- E],
  ok.

%%%=====================================================================================================================

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
%%%=====================================================================================================================

%%%===================================================================
%%% Internal functions
%%%===================================================================

% rtsource_conn_mgr callback
start_rtsource_conn(Remote, S) ->
  lager:info("Adding a connection and starting rtsource_conn ~p", [Remote]),
  Args = [Remote, self()],
  supervisor:start_child(S, Args).


maybe_rebalance(State, now) ->
  {NewSource, NewSink} = get_source_and_sink_nodes(State#state.remote),
  case should_rebalance(State, NewSource, NewSink) of
    false ->
      lager:debug("rebalancing triggered but there is no change in source or sink node status"),
      State;

    {true, {equal, _DropNodes, _ConnectToNodes, _Primary, _Secondary, _ConnectedSinkNodes}} ->
      lager:debug("rebalancing triggered but avoided via active connection matching"),
      State;

    {true, {nodes_up, _DropNodes, ConnectToNodes, _Primary, _Secondary, _ConnectedSinkNodes}} ->
      lager:debug("rebalancing triggered and new connections are required"),
      NewState1 = check_remove_endpoint(State, ConnectToNodes),
      rebalance_connect(NewState1#state{sink_nodes = NewSink, source_nodes = NewSource}, ConnectToNodes);

    {true, {nodes_down, DropNodes, _ConnectToNodes, _Primary, _Secondary, ConnectedSinkNodes}} ->
      lager:debug("rebalancing triggered and active connections required to be dropped"),
      {_RemoveAllConnections, NewState1} = check_and_drop_connections(State, DropNodes, ConnectedSinkNodes),
      riak_repl2_rtsource_conn_data_mgr:write(realtime_connections, NewState1#state.remote, node(), orddict:fetch_keys(NewState1#state.endpoints)),
      NewState1#state{sink_nodes = NewSink, source_nodes = NewSource};

    {true, {nodes_up_and_down, DropNodes, ConnectToNodes, Primary, Secondary, ConnectedSinkNodes}} ->
      lager:debug("rebalancing triggered and some active connections required to be dropped, and also new connections to be made"),
      {RemoveAllConnections, NewState1} = check_and_drop_connections(State, DropNodes, ConnectedSinkNodes),
      NewState2 = check_remove_endpoint(NewState1, ConnectToNodes),
      riak_repl2_rtsource_conn_data_mgr:write(realtime_connections, NewState2#state.remote, node(), orddict:fetch_keys(NewState2#state.endpoints)),
      case RemoveAllConnections of
        true ->
          rebalance_connect(NewState2#state{sink_nodes = NewSink, source_nodes = NewSource}, Primary++Secondary);
        false ->
          rebalance_connect(NewState2#state{sink_nodes = NewSink, source_nodes = NewSource}, ConnectToNodes)
      end
  end;
maybe_rebalance(State, delayed) ->
  case State#state.rb_timeout_tref of
    undefined ->
      RbTimeoutTref = erlang:send_after(State#state.rebalance_delay, self(), rebalance_now),
      State#state{rb_timeout_tref = RbTimeoutTref};
    _ ->
      %% Already sent a "rebalance_now"
      State
  end.


should_rebalance(#state{endpoints = Endpoints, remote=Remote, sink_nodes = OldSink, source_nodes = OldSource}, NewSource, NewSink) ->

  {SourceComparison, _SourceNodesDown, _SourceNodesUp} = compare_nodes(OldSource, NewSource),
  {SinkComparison, _SinkNodesDown, _SinkNodesUp} = compare_nodes(OldSink, NewSink),

  lager:debug("zzz source comparison = ~p", [SourceComparison]),
  lager:debug("zzz sink comparison = ~p", [SinkComparison]),

  case {SourceComparison, SinkComparison} of
    {equal, equal} ->
      false;
    _ ->
      case riak_core_cluster_mgr:get_ipaddrs_of_cluster(Remote, split) of
        {ok, []} ->
          false;
        {ok, {Primary, Secondary}} ->

          lager:debug("conn_mgr endpoints ~p", [Endpoints]),

          ConnectedSinkNodes = [ {IPPort, P} || {{IPPort, P},_Pid} <- orddict:to_list(Endpoints)],
          {Action, DropNodes, ConnectToNodes} = compare_nodes(ConnectedSinkNodes, Primary),
          lager:debug("www
          New connections: ~p
          Old connections: ~p
          Action: ~p
          Drop Nodes: ~p
          Connect to Nodes: ~p", [Primary, ConnectedSinkNodes, Action, DropNodes, ConnectToNodes]),
          {true, {Action, DropNodes, ConnectToNodes, Primary, Secondary, ConnectedSinkNodes}}
      end

  end.

check_remove_endpoint(State=#state{remove_endpoint = RE}, ConnectToNodes) ->
  case lists:member(RE, ConnectToNodes) of
    true ->
      State#state{remove_endpoint = undefined};
    false ->
      State
  end.


check_and_drop_connections(State=#state{endpoints = E}, DropNodes=[X|Xs], ConnectedSinkNodes) ->
  case ConnectedSinkNodes -- DropNodes of
    [] ->
      NewEndpoints = remove_connections(Xs, E),
      {true, State#state{endpoints = NewEndpoints, remove_endpoint = X}};
    _ ->
      NewEndpoints = remove_connections(DropNodes, E),
      {false, State#state{endpoints = NewEndpoints, remove_endpoint = undefined}}
  end.

remove_connections([], E) ->
  E;
remove_connections([Key | Rest], E) ->
  RtSourcePid = orddict:fetch(Key, E),
  HelperPid = riak_repl2_rtsource_conn:get_helper_pid(RtSourcePid),
  riak_repl2_rtsource_helper:stop_pulling(HelperPid),
  lager:debug("rtsource_conn called to gracefully kill itself ~p", [Key]),
  erlang:send_after(?KILL_TIME, self(), {kill_rtsource_conn, RtSourcePid}),
  remove_connections(Rest, orddict:erase(Key, E)).

get_source_and_sink_nodes(Remote) ->
  SourceNodes = riak_repl2_rtsource_conn_data_mgr:read(active_nodes),
  SinkNodes = riak_core_cluster_mgr:get_unsuhffled_ipaddrs_of_cluster(Remote),
  {SourceNodes, SinkNodes}.

compare_nodes(Old, New) ->
  case Old == New of
    true ->
      {equal, [],[]};
    false ->
      {NodesDownRes, NodesDown} = diff_nodes(Old, New),
      {NodesUpRes, NodesUp} = diff_nodes(New, Old),
      case {NodesDownRes, NodesUpRes} of
        {true, true} ->
          {nodes_up_and_down, NodesDown, NodesUp};
        {true, false} ->
          {nodes_down, NodesDown, NodesUp};
        {false, true} ->
          {nodes_up, NodesDown, NodesUp};
        {false, false} ->
          %% we should never reach these case statement
          {equal, NodesDown, NodesUp}
      end
  end.

diff_nodes(N1, N2) ->
  case N1 -- N2 of
    [] ->
      {false, []};
    X ->
      {true, X}
  end.

rebalance_connect(State=#state{remote=Remote}, BetterAddrs) ->
  lager:info("trying reconnect to one of: ~p", [BetterAddrs]),

  %% if we have a pending connection attempt - drop that
  riak_core_connection_mgr:disconnect({rt_repl, Remote}),

  lager:debug("re-connecting to remote ~p", [Remote]),
  case riak_core_connection_mgr:connect({rt_repl, Remote}, ?CLIENT_SPEC, {use_only, BetterAddrs}) of
    {ok, Ref} ->
      lager:debug("connecting ref ~p", [Ref]),

      lager:debug("rebalanced is complete"),

      State#state{connection_ref = Ref};
    {error, Reason}->
      lager:warning("Error connecting to remote ~p (ignoring as we're reconnecting)", [Reason]),
      State
  end.

collect_status_data([], Data, _E) ->
  Data;
collect_status_data([Key | Rest], Data, E) ->
  Pid = orddict:fetch(Key, E),
  NewData = [riak_repl2_rtsource_conn:status(Pid) | Data],
  collect_status_data(Rest, NewData, E).