-module(riak_repl2_rtsource_remote_conn_sup).
-author("nordine saadouni").

-behaviour(supervisor).

%% API
-export([start_link/1,
  make_module_name/1,
  get_conn_mgr_status/1
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

get_conn_mgr_status(Pid) ->
 ConnMgr = riak_repl2_rtsource_conn_sup:first_or_empty([ P || {_, P, _, [riak_repl2_rtsource_conn_mgr]} <-
   supervisor:which_children(Pid), is_pid(P)]),
  case ConnMgr of
    [] ->
     [];
    X ->
      riak_repl2_rtsource_conn_mgr:get_all_status(X)
  end.


%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([Remote]) ->
  ConnMgr = {Remote, {riak_repl2_rtsource_conn_mgr, start_link, [[Remote]]},
    permanent, ?SHUTDOWN, worker, [riak_repl2_rtsource_conn_mgr]},

  ConnSup = {riak_repl2_rtsource_conn_2_sup:make_module_name(Remote), {riak_repl2_rtsource_conn_2_sup, start_link, [Remote]},
    permanent, ?SHUTDOWN, supervisor, [riak_repl2_rtsource_conn_2_sup]},

  {ok, {{one_for_all, 10, 10}, [ConnSup, ConnMgr]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

make_module_name(Remote) ->
  list_to_atom(lists:flatten(io_lib:format("riak_repl2_rtsource_remote_conn_sup_~s", [Remote]))).