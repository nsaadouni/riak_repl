-module(riak_repl2_rtsource_conn_2_sup).
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


start_link(Remote) ->
  supervisor:start_link({local, ?SERVER(Remote)}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([]) ->

  Conn = {riak_repl2_rtsource_conn, {riak_repl2_rtsource_conn, start_link, []},
    transient, ?SHUTDOWN, worker, [riak_repl2_rtsource_conn]},

  {ok, {{simple_one_for_one, 10, 10}, [Conn]}}.



%%%===================================================================
%%% Internal functions
%%%===================================================================

make_module_name(Remote) ->
  list_to_atom(lists:flatten(io_lib:format("riak_repl2_rtsource_conn_2_~p_sup", [Remote]))).