%% Realtime Source Connection Manager
%% This is a worker that takes away some of the responsability that belonged to rtsource_conn

%% Here we will connect to the remote sink cluster
%% recieve connections from riak_core_connection_mgr and start children (rtsource_conn) for each connection
%% We save the state of the connections that have been made and complete rebalacning in this gen_server

-module(riak_repl2_rtsource_conn_mgr).
-author("nordine saadouni"). % this will be altered as I am taking code from rtsource_conn to place in here

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
  kill_connection/2,
  should_rebalance/1,
  stop/1,
  get_all_status/1,
  get_all_status/2
]).

-define(SERVER, ?MODULE).
-define(SHUTDOWN, 5000). % how long to give rtsource processes to persist queue/shutdown

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
  rebalance_flag = false,


  endpoints
  % Want a store for the following information
  % Key = {IP, Port}; Value = {Primary, Pid}    [Pid will be the Pid of the rtsource_conn]
  % This information will aid the reblancing process!

  % In the supervisor we need a method of gracefully killing a child (will add that to rtsource_conn as a callback)
  % Rebalancing will find new best buddies, attempt the connection, if not secondary then we replace them!

}).

%%%===================================================================
%%% API
%%%===================================================================


start_link([Remote]) ->
  gen_server:start_link(?MODULE, [Remote], []).

%%make_module_name(Remote) ->
%%  list_to_atom(lists:flatten(io_lib:format("riak_repl2_rtsource_conn_mgr_~s", [Remote]))).


% Replacement for connected from rtsource_conn! (This needs to be changed in riak_core_connection (gen_fsm)
% I do not have information regarding the connection type (primary or secondary)
% I can pass it to here or request it from the core_connection_mgr (Either way I need it)
connected(Socket, Transport, IPPort, Proto, RTSourceConnMgrPid, _Props, Primary) ->
  Transport:controlling_process(Socket, RTSourceConnMgrPid),
  gen_server:call(RTSourceConnMgrPid, {connected, Socket, Transport, IPPort, Proto, _Props, Primary}).

connect_failed(_ClientProto, Reason, RTSourceConnMgrPid) ->
  gen_server:cast(RTSourceConnMgrPid, {connect_failed, self(), Reason}).


kill_connection(ConnMgrPid, RtsourcePid) ->
  lager:debug("kill connection successful"),
  gen_server:cast(ConnMgrPid, {kill_rtsource_conn, RtsourcePid}).

%% @doc Check if we need to rebalance.
%% If we do, delay some time, recheck that we still
%% need to rebalance, and if we still do, then execute
%% reconnection to the better sink node.
maybe_rebalance_delayed(Pid) ->
  gen_server:cast(Pid, rebalance_delayed).

stop(Pid) ->
  gen_server:call(Pid, stop).

get_all_status(Pid) ->
  get_all_status(Pid, infinity).
get_all_status(Pid, Timeout) ->
  gen_server:call(Pid, all_status, Timeout).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


init([Remote]) ->
  %% Todo: check for bad remote name
  lager:debug("connecting to remote ~p", [Remote]),
  case riak_core_connection_mgr:connect({rt_repl, Remote}, ?CLIENT_SPEC, multi_connection) of
    {ok, Ref} ->
      _ = riak_repl2_rtq:register(Remote), % re-register to reset stale deliverfun
      lager:debug("connection ref ~p", [Ref]),
      S = riak_repl2_rtsource_conn_2_sup:make_module_name(Remote),
      E = orddict:new(),

      MaxDelaySecs = app_helper:get_env(riak_repl, realtime_connection_rebalance_max_delay_secs, 60),
      M = round(MaxDelaySecs * crypto:rand_uniform(0, 1000)),

      {ok, #state{remote = Remote, connection_ref = Ref, rtsource_conn_sup = S, endpoints = E, rebalance_delay = M}};
    {error, Reason}->
      lager:warning("Error connecting to remote"),
      {stop, Reason}
  end.

%%%=====================================================================================================================
handle_call({connected, Socket, Transport, IPPort, Proto, _Props, Primary}, _From,
    State = #state{rtsource_conn_sup = S, remote = Remote, endpoints = E, rebalance_flag = RF}) ->

  lager:debug("rtsource_conn_mgr connection recieved"),

  case start_rtsource_conn(Remote, S) of
    {ok, RtSourcePid} ->
      case riak_repl2_rtsource_conn:connected(Socket, Transport, IPPort, Proto, RtSourcePid, _Props, Primary) of
        ok ->

          % check if this is a rebalanced connection
          Endpoints = case RF of
            true ->
              remove_all_current_connections(State),
              orddict:new();
            false ->
              E
          end,

          %% Save {EndPoint, Pid}; Pid will come from the supervisor starting a child
          NewEndpoints = orddict:store({IPPort, Primary}, RtSourcePid, Endpoints),

          % save to ring
          lager:debug("rtsource_conn_mgr send connection data to data mgr"),
          riak_repl2_rtsource_conn_data_mgr:write(realtime_connections, Remote, node(), IPPort, Primary),

          {reply, ok, State#state{endpoints = NewEndpoints, rebalance_flag = false}};

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

handle_call(stop, _From, State) ->
  {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

%%%=====================================================================================================================

%% Connection manager failed to make connection
handle_cast({connect_failed, _HelperPid, Reason},
    State = #state{remote = Remote}) ->
  lager:warning("Realtime replication connection to site ~p failed - ~p\n",
    [Remote, Reason]),
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


handle_info(_Info, State) ->
  {noreply, State}.

%%%=====================================================================================================================

terminate(_Reason, _State=#state{remote = Remote, endpoints = E}) ->
  lager:debug("rtrsource_conn_mgr terminating"),
  %% consider unregistering from rtq!
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
  Args = [Remote],
  supervisor:start_child(S, Args).

remove_all_current_connections(_State=#state{endpoints = E, remote = R}) ->
  AllKeys = orddict:fetch_keys(E),
  lager:debug("realtime node connections deleted"),
  riak_repl2_rtsource_conn_data_mgr:delete(realtime_connections, R, node()),
  remove_connection(AllKeys, E).

remove_connection([], _) ->
  ok;
remove_connection([Key | Rest], E) ->
  RtsourcePid = orddict:fetch(Key, E),
  lager:debug("rtsource_conn called to gracefully kill itself ~p", [Key]),
  riak_repl2_rtsource_conn:kill_connection_gracefully(RtsourcePid, self()),
  remove_connection(Rest, E).



maybe_rebalance(State, now) ->
  case should_rebalance(State) of
    no ->
      State;
    {yes, UsefulAddrs} ->
      lager:debug("we are rebalancing"),
      reconnect(State, UsefulAddrs)
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


should_rebalance(#state{endpoints = E, remote=Remote}) ->

  case riak_core_cluster_mgr:get_ipaddrs_of_cluster_multifix(Remote, primary) of
    {ok, []} ->
      no;
    {ok, Primaries} ->
      check_addrs(orddict:fetch_keys(E), Primaries)
  end.

check_addrs(Current, New) ->
  case check_addrs_helper(Current, New, true) of
    true ->
      no;
    false ->
      UsefulAddrs = riak_core_connection_mgr:filter_blacklisted_ipaddrs(New),
      case UsefulAddrs of
        [] ->
          no;
        X ->
          {yes, X}
      end
  end.


check_addrs_helper(_, _, false) ->
  false;
check_addrs_helper([], _, true) ->
  true;
check_addrs_helper([Addr| Addrs], New, true) ->
  check_addrs_helper(Addrs, New, lists:member(Addr, New)).




reconnect(State=#state{remote=Remote}, BetterAddrs) ->
  lager:info("trying reconnect to one of: ~p", [BetterAddrs]),

  %% if we have a pending connection attempt - drop that
  riak_core_connection_mgr:disconnect({rt_repl, Remote}),

  lager:debug("re-connecting to remote ~p", [Remote]),
  case riak_core_connection_mgr:connect({rt_repl, Remote}, ?CLIENT_SPEC, {use_only, BetterAddrs}) of
    {ok, Ref} ->
      lager:debug("connecting ref ~p", [Ref]),

      lager:debug("rebalanced is complete"),

      State#state{ connection_ref = Ref, rebalance_flag = true};
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




%% ================================================================================================================
%%rebalance_delay_millis() ->
%%  MaxDelaySecs =
%%    app_helper:get_env(riak_repl, realtime_connection_rebalance_max_delay_secs, 5*60),
%%  round(MaxDelaySecs * crypto:rand_uniform(0, 1000)).