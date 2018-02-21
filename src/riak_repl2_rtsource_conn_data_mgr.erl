-module(riak_repl2_rtsource_conn_data_mgr).
-author("nordine saadouni").

-behaviour(gen_server).

%% API
-export([start_link/0,
  set_leader/2,
  write/4,
  delete/1

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
  connections

}).

%%%===================================================================
%%% API
%%%===================================================================
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

set_leader(LeaderNode, _LeaderPid) ->
  gen_server:cast(?SERVER, {set_leader_node, LeaderNode}).

write(Remote, Node, IPPort, Primary) ->
  gen_server:cast(?SERVER, {write, Remote, Node, IPPort, Primary}).

delete(Remote) ->
  gen_server:cast(?SERVER, {delete, Remote}).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->

  Leader = undefined,
  IsLeader = false,
  C = dict:new(),

  lager:debug("conn_data_mgr started"),
  riak_core_ring_manager:ring_trans(fun riak_repl_ring:delete_realtime_connection_data/2, all),

  {ok, #state{is_leader = IsLeader, leader_node = Leader, connections = C}}.


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

handle_cast(Msg = {delete, Remote}, State=#state{connections = C, is_leader = L}) ->
  case L of
    true ->
      NewC = dict:erase(Remote,C),
      riak_core_ring_manager:ring_trans(fun riak_repl_ring:delete_realtime_connection_data/2,
      {Remote}),
      {noreply, State#state{connections = NewC}};
    false ->
      proxy_cast(Msg, State),
      {noreply, State}
  end;


handle_cast(Msg = {write, Remote, Node, IPPort, Primary}, State=#state{connections = C}) ->
  case State#state.is_leader of
    true ->

      lager:debug("write leader messages: ~p", [Msg]),

      OldRemoteDict = get_value(Remote, C, dictionary),
      OldConnsList = get_value(Node, OldRemoteDict, list),

      NewConnsList = [{IPPort, Primary} |OldConnsList],
      NewRemoteDict = dict:store(Node, NewConnsList, OldRemoteDict),
      NewConnections = dict:store(Remote, NewRemoteDict, C),

      % push onto ring
      riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_realtime_connection_data/2,
        NewConnections),

      {noreply, State#state{connections = NewConnections}};

    false ->
      lager:debug("write proxy message: ~p", [Msg]),
      proxy_cast(Msg, State),
      {noreply, State}
  end;

handle_cast(_Request, State) ->
  {noreply, State}.


handle_info(restore_realtime_connection_info, State) ->
  RemoteRealtimeConnections = riak_repl_ring:get_realtime_connection_data(),
  {noreply, State#state{connections = RemoteRealtimeConnections}};

handle_info(_Info, State) ->
  {noreply, State}.


terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
%% start being a cluster manager leader
become_leader(State, _LeaderNode) when State#state.is_leader == false ->
  erlang:send_after(5000, self(), restore_realtime_connection_info),
  State#state{is_leader = true};
become_leader(State, _LeaderNode) ->
  State.

%% stop being a cluster manager leader
become_proxy(State, _LeaderNode) when State#state.is_leader == true ->
%%  erlang:send_after(5000, self(), update_new_leader),
  State#state{is_leader = false};
become_proxy(State, _LeaderNode) ->
%%  erlang:send_after(5000, self(), update_new_leader),
  State.


proxy_cast(_Cast, _State = #state{leader_node=Leader}) when Leader == undefined ->
  ok;
proxy_cast(Cast, _State = #state{leader_node=Leader}) ->
  gen_server:cast({?SERVER, Leader}, Cast).
%%
%%proxy_call(_Call, NoLeaderResult, State = #state{leader_node=Leader}) when Leader == undefined ->
%%  {reply, NoLeaderResult, State};
%%proxy_call(Call, NoLeaderResult, State = #state{leader_node=Leader}) ->
%%  Reply = try gen_server:call({?SERVER(Remote), Leader}, Call, ?PROXY_CALL_TIMEOUT) of
%%            R -> R
%%          catch
%%            exit:{noproc, _} ->
%%              NoLeaderResult;
%%            exit:{{nodedown, _}, _} ->
%%              NoLeaderResult
%%          end,
%%  {reply, Reply, State}.
%%
%%

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