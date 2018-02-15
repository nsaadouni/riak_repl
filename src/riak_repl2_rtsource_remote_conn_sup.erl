-module(riak_repl2_rtsource_remote_conn_sup).
-author("nordine saadouni").

-behaviour(supervisor).

%% API
-export([start_link/1,
  make_module_name/1
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

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([Remote]) ->
  ConnMgr = {make_conn_mgr_name(Remote), {riak_repl2_rtsource_conn_mgr, start_link, [[Remote]]},
    permanent, ?SHUTDOWN, worker, [riak_repl2_rtsource_conn_mgr]},

  ConnSup = {make_conn_2_sup_name(Remote), {riak_repl2_rtsource_conn_2_sup, start_link, [Remote]},
    permanent, ?SHUTDOWN, supervisor, [riak_repl2_rtsource_conn_2_sup]},

  % delete any data held on ring about connections for this remote cluster
  riak_core_ring_manager:ring_trans(fun riak_repl_ring:delete_connection_data/2,
    {Remote}),


  {ok, {{one_for_one, 10, 10}, [ConnSup, ConnMgr]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

make_module_name(Remote) ->
  list_to_atom(lists:flatten(io_lib:format("riak_repl2_rtsource_remote_conn_sup_~s", [Remote]))).

make_conn_mgr_name(Remote) ->
  list_to_atom(lists:flatten(io_lib:format("conn_mgr_~s", [Remote]))).

make_conn_2_sup_name(Remote) ->
  list_to_atom(lists:flatten(io_lib:format("conn_2_sup_~s", [Remote]))).