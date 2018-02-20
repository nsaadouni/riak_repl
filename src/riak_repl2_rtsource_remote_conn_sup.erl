-module(riak_repl2_rtsource_remote_conn_sup).
-author("nordine saadouni").

-behaviour(supervisor).

%% API
-export([start_link/1,
  make_module_name/1,
  set_leader/2
]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER(Remote), make_module_name(Remote)).
-define(SHUTDOWN, 5000). % how long to give rtsource processes to persist queue/shutdown

%%%===================================================================
%%% API functions
%%%===================================================================

start_link(Remote) ->
  supervisor:start_link({local, ?SERVER(Remote) }, ?MODULE, [Remote]).

set_leader(LeaderNode, _LeaderPid) ->
  [riak_repl2_rtsource_conn_mgr:set_leader(Pid, LeaderNode, _LeaderPid) || {_, Pid, worker, riak_repl2_rtsource_conn_mgr} <- supervisor:which_children(?MODULE)].



%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([Remote]) ->
  ConnMgr = {make_module_name_conn(Remote), {riak_repl2_rtsource_conn_mgr, start_link, [[Remote]]},
    permanent, ?SHUTDOWN, worker, [riak_repl2_rtsource_conn_mgr]},

  ConnSup = {riak_repl2_rtsource_conn_2_sup:make_module_name(Remote), {riak_repl2_rtsource_conn_2_sup, start_link, [Remote]},
    permanent, ?SHUTDOWN, supervisor, [riak_repl2_rtsource_conn_2_sup]},

  {ok, {{one_for_one, 10, 10}, [ConnSup, ConnMgr]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

make_module_name(Remote) ->
  list_to_atom(lists:flatten(io_lib:format("riak_repl2_rtsource_remote_conn_sup_~s", [Remote]))).

make_module_name_conn(Remote) ->
  list_to_atom(lists:flatten(io_lib:format("riak_repl2_rtsource_conn_mgr_~s", [Remote]))).