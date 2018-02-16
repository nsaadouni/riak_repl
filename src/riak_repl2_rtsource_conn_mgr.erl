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
  kill_connection/2
]).

-define(SERVER, ?MODULE).

-define(SHUTDOWN, 5000). % how long to give rtsource processes to persist queue/shutdown

-define(CLIENT_SPEC, {{realtime,[{3,0}, {2,0}, {1,5}]},
  {?TCP_OPTIONS, ?MODULE, self()}}).

-define(TCP_OPTIONS,  [{keepalive, true},
  {nodelay, true},
  {packet, 0},
  {active, false}]).

-record(state, {
  remote, % remote sink cluster name
  connection_ref, % reference handed out by connection manager
  rtsource_conn_sup, % The module name of the supervisor to start the child when we get a connection passed to us
  rb_timeout_tref, % Rebalance timeout timer reference


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

% Replacement for connected from rtsource_conn! (This needs to be changed in riak_core_connection (gen_fsm)
% I do not have information regarding the connection type (primary or secondary)
% I can pass it to here or request it from the core_connection_mgr (Either way I need it)
connected(Socket, Transport, IPPort, Proto, RTSourceConnMgrPid, _Props, Primary) ->
  Transport:controlling_process(Socket, RTSourceConnMgrPid),
  gen_server:call(RTSourceConnMgrPid, {connected, Socket, Transport, IPPort, Proto, _Props, Primary}).

connect_failed(_ClientProto, Reason, RTSourceConnMgrPid) ->
  gen_server:cast(RTSourceConnMgrPid, {connect_failed, self(), Reason}).


kill_connection(Pid, Addr) ->
  gen_server:cast(Pid, {kill, Addr}).

%% @doc Check if we need to rebalance.
%% If we do, delay some time, recheck that we still
%% need to rebalance, and if we still do, then execute
%% reconnection to the better sink node.
maybe_rebalance_delayed(_Pid) ->
  ok.
%%  gen_server:cast(Pid, rebalance_delayed).


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
      {ok, #state{remote = Remote, connection_ref = Ref, rtsource_conn_sup = S, endpoints = E}};
    {error, Reason}->
      lager:warning("Error connecting to remote"),
      {stop, Reason}
  end.

%%%=====================================================================================================================
handle_call({connected, Socket, Transport, IPPort, Proto, _Props, Primary}, _From,
    State = #state{rtsource_conn_sup = S, remote = Remote, endpoints = E}) ->

  case start_rtsource_conn(Remote, S) of
    {ok, RtSourcePid} ->
      case riak_repl2_rtsource_conn:connected(Socket, Transport, IPPort, Proto, RtSourcePid, _Props) of
        ok ->
          %% Save {EndPoint, Pid}; Pid will come from the supervisor starting a child
          Endpoints = orddict:store(IPPort, {RtSourcePid, Primary}, E),

          %% Save this also to the ring
          riak_core_cluster_mgr:add_realtime_connection_data(Remote, node(), IPPort, Primary, append),

          {reply, ok, State#state{endpoints = Endpoints}};

        Error ->
          % This is the error that causes the RtSource to exit and kill itself
          % When this happens the supervisor will not restart
          % It used to restart it; and it would therefore re-attempt the connection!
          % riak_core_connection_mgr:connect/3 would be called again
          % I need to figure out a method of re-trying this

          % Unable to contact RT Source connection process thats why (This should not be an issue!)
          % We have just booted up!
          % could simply make another call?
          {reply, Error, State}
      end;
    ER ->
      {reply, ER, State}
  end;

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

%%%=====================================================================================================================

%% Connection manager failed to make connection
%% TODO: Consider reissuing connect against another host - maybe that
%%   functionality should be in the connection manager (I want a connection to site X)
handle_cast({connect_failed, _HelperPid, Reason},
    State = #state{remote = Remote}) ->
  lager:warning("Realtime replication connection to site ~p failed - ~p\n",
    [Remote, Reason]),
  {stop, normal, State};

handle_cast({kill, Addr}, State = #state{endpoints = E}) ->
  case orddict:fetch(Addr, E) of
    {Pid, _Primary} ->
      riak_repl2_rtsource_conn:stop(Pid),
      E1 = orddict:erase(Addr, E),
      {noreply, State#state{endpoints = E1}};
    _ ->
      % rtsource_conn has stopped pulling from queue but it is not in our endpoints dictionary!
      % we should never reach this case
      {noreply, State}
  end;


%%handle_cast(rebalance_delayed, State) ->
%%  {noreply, maybe_rebalance(State, delayed)};


handle_cast(_Request, State) ->
  {noreply, State}.

%%%=====================================================================================================================

%%handle_info(rebalance_now, State) ->
%%  {noreply, maybe_rebalance(State#state{rb_timeout_tref = undefined}, now)};

handle_info(_Info, State) ->
  {noreply, State}.

%%%=====================================================================================================================

terminate(_Reason, _State=#state{remote = Remote, endpoints = E}) ->
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

% rtsource_conn_mgr callback
%% need to check if this will work with stop, or if I need to code some more in rtsource_conn to
%% kill the process as well.
%%remove_connection(_Pid) ->
%%  ok.

%%maybe_rebalance(State, now) ->
%%  case should_rebalance(State) of
%%    no ->
%%      State;
%%    {yes, UsefulAddrs} ->
%%      reconnect(State, UsefulAddrs)
%%  end;
%%maybe_rebalance(State, delayed) ->
%%  case State#state.rb_timeout_tref of
%%    undefined ->
%%      RbTimeoutTref = erlang:send_after(rebalance_delay_millis(), self(), rebalance_now),
%%      State#state{rb_timeout_tref = RbTimeoutTref};
%%    _ ->
%%      %% Already sent a "rebalance_now"
%%      State
%%  end.
%%
%%%% This needs to be fixed, for now I will keep it as is, fix the connections then move onto this
%%should_rebalance(#state{address=ConnectedAddr, remote=Remote}) ->
%%  {ok, Ring} = riak_core_ring_manager:get_my_ring(),
%%  Addrs = riak_repl_ring:get_clusterIpAddrs(Ring, Remote),
%%  {ok, ShuffledAddrs} = riak_core_cluster_mgr:get_my_remote_ip_list(Addrs),
%%  lager:debug("ShuffledAddrs: ~p, ConnectedAddr: ~p", [ShuffledAddrs, ConnectedAddr]),
%%
%%  % This case statement will now always return false due to shuffledAddrs having a different return type!
%%  case (ShuffledAddrs /= []) andalso same_ipaddr(ConnectedAddr, hd(ShuffledAddrs)) of
%%    true ->
%%      no; % we're already connected to the ideal buddy
%%    false ->
%%      %% compute the addrs that are "better" than the currently connected addr
%%      BetterAddrs = lists:filter(fun(A) -> not same_ipaddr(ConnectedAddr, A) end,
%%        ShuffledAddrs),
%%      %% remove those that are blacklisted anyway
%%      UsefulAddrs = riak_core_connection_mgr:filter_blacklisted_ipaddrs(BetterAddrs),
%%      lager:debug("BetterAddrs: ~p, UsefulAddrs ~p", [BetterAddrs, UsefulAddrs]),
%%      case UsefulAddrs of
%%        [] ->
%%          no;
%%        UsefulAddrs ->
%%          {yes, UsefulAddrs}
%%      end
%%  end.
%%
%%rebalance_delay_millis() ->
%%  MaxDelaySecs =
%%    app_helper:get_env(riak_repl, realtime_connection_rebalance_max_delay_secs, 5*60),
%%  round(MaxDelaySecs * crypto:rand_uniform(0, 1000)).
%%
%%reconnect(State=#state{remote=Remote}, BetterAddrs) ->
%%  lager:info("trying reconnect to one of: ~p", [BetterAddrs]),
%%
%%  %% if we have a pending connection attempt - drop that
%%  riak_core_connection_mgr:disconnect({rt_repl, Remote}),
%%
%%  lager:debug("re-connecting to remote ~p", [Remote]),
%%  case riak_core_connection_mgr:connect({rt_repl, Remote}, ?CLIENT_SPEC, {use_only, BetterAddrs}) of
%%    {ok, Ref} ->
%%      lager:debug("connecting ref ~p", [Ref]),
%%      State#state{ connection_ref = Ref};
%%    {error, Reason}->
%%      lager:warning("Error connecting to remote ~p (ignoring as we're reconnecting)", [Reason]),
%%      State
%%  end.
%%
%%% CC
%%% This needs changed now, will do it when I work on re-balance connections
%%same_ipaddr({IP,Port}, {IP,Port}) ->
%%  true;
%%same_ipaddr({_IP1,_Port1}, {_IP2,_Port2}) ->
%%  false;
%%same_ipaddr(X,Y) ->
%%  lager:warning("ipaddrs have unexpected format! ~p, ~p", [X,Y]),
%%  false.