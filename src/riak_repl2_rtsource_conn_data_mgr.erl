-module(riak_repl2_rtsource_conn_data_mgr).
-author("nordine saadouni").

-behaviour(gen_server).

%% API
-export([
  start_link/0,
  set_leader/2,

  delete/2, delete/3, delete/5,
  read/1, read/2, read/3,
  write/5
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
-define(NODE_WATCHER_POLLING_INTERVAL, 10*1000).

-record(state, {

  leader_node,
  is_leader,
  connections,
  active_nodes

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
  Leader = undefined,
  IsLeader = false,
  C = dict:new(),
  AN = [],
  lager:debug("conn_data_mgr started"),
  riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_realtime_connection_data/2, C),
  erlang:send_after(?NODE_WATCHER_POLLING_INTERVAL, self(), poll_node_watcher),
  {ok, #state{is_leader = IsLeader, leader_node = Leader, connections = C, active_nodes = AN}}.


%% -------------------------------------------------- Read ---------------------------------------------------------- %%

handle_call(Msg={read_realtime_connections, Remote}, _From, State=#state{connections = C, is_leader = L}) ->
  case L of
    true ->
      NodeDict = get_value(Remote, C, dictionary),
      {reply, NodeDict, State};
    false ->
      NoLeaderResult = {ok, []},
      proxy_call(Msg, NoLeaderResult, State)
  end;

handle_call(Msg={read_realtime_connections, Remote, Node}, _From, State=#state{connections = C, is_leader = L}) ->
  case L of
    true ->
      NodeDict = get_value(Remote, C, dictionary),
      ConnsList = get_value(Node, NodeDict, list),
      {reply, ConnsList, State};
    false ->
      NoLeaderResult = {ok, []},
      proxy_call(Msg, NoLeaderResult, State)
  end;

handle_call(Msg=read_active_nodes, _From, State=#state{active_nodes = AN, is_leader = L}) ->
  case L of
    true ->
      {reply, AN, State};
    false ->
      NoLeaderResult = {ok, []},
      proxy_call(Msg, NoLeaderResult, State)
  end;


handle_call(_Request, _From, State) ->
  {reply, ok, State}.


handle_cast({set_leader_node, LeaderNode}, State) ->
  State2 = State#state{leader_node = LeaderNode},
  case node() of
    LeaderNode ->
      {noreply, become_leader(State2, LeaderNode)};
    _ ->
      {noreply, become_proxy(State2, LeaderNode)}
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
      lager:debug("delted realtime connection data ~p ~p", [Remote, Node]),

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


handle_cast(_Request, State) ->
  {noreply, State}.


handle_info(restore_leader_data, State) ->
  RemoteRealtimeConnections = riak_repl_ring:get_realtime_connection_data(),
  ActiveNodes = riak_repl_ring:get_active_nodes(),
  {noreply, State#state{connections = RemoteRealtimeConnections, active_nodes = ActiveNodes}};

handle_info(poll_node_watcher, State=#state{active_nodes = AN, connections = C}) when State#state.is_leader == true ->
  ActiveNodes = riak_core_node_watcher:nodes(riak_kv),
  Connections = case ActiveNodes == AN of

                 true ->
                   C;
                 false ->
                   riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_active_nodes/2, ActiveNodes),
                   case AN -- ActiveNodes of
                     [] ->
                       C;
                     DownNodes ->
                       AllRemotes = dict:fetch_keys(C),
                       lager:debug("down nodes = ~p; known remotes = ~p; connections = ~p", [DownNodes, AllRemotes, C]),
                       NewC = remove_nodes_remotes(DownNodes, AllRemotes, C),
                       lager:debug("down nodes, new connections dictionary ~p", [NewC]),
                       riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_realtime_connection_data/2, NewC),
                       NewC
                   end
               end,
  erlang:send_after(?NODE_WATCHER_POLLING_INTERVAL, self(), poll_node_watcher),
  {noreply, State#state{active_nodes = ActiveNodes, connections = Connections}};
handle_info(poll_node_watcher, State) ->
  erlang:send_after(?NODE_WATCHER_POLLING_INTERVAL, self(), poll_node_watcher),
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

%% start being a cluster manager leader
become_leader(State, _LeaderNode) when State#state.is_leader == false ->
  erlang:send_after(5000, self(), restore_leader_data),
  State#state{is_leader = true};
become_leader(State, _LeaderNode) ->
  State.

%% stop being a cluster manager leader
become_proxy(State, _LeaderNode) when State#state.is_leader == true ->
%%  update the ring?
%%  erlang:send_after(5000, self(), update_ring_delete_connection_data),
  State#state{is_leader = false};
become_proxy(State, _LeaderNode) ->
%%  erlang:send_after(5000, self(), update_new_leader),
  State.


proxy_cast(_Cast, _State = #state{leader_node=Leader}) when Leader == undefined ->
  ok;
proxy_cast(Cast, _State = #state{leader_node=Leader}) ->
  gen_server:cast({?SERVER, Leader}, Cast).

proxy_call(_Call, NoLeaderResult, State = #state{leader_node=Leader}) when Leader == undefined ->
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