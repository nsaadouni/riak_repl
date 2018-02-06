-module(riak_repl2_rtsource_remote_conn_sup).
-author("nordine saadouni").

-behaviour(supervisor).

%% API
-export([start_link/1,
  remove_connection/1
]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).
-define(SHUTDOWN, 5000). % how long to give rtsource processes to persist queue/shutdown

%%%===================================================================
%%% API functions
%%%===================================================================

start_link(Remote) ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, [Remote]).

% rtsource_conn_mgr callback
%% need to check if this will work with stop, or if I need to code some more in rtsource_conn to
%% kill the process as well.
remove_connection(_Pid) ->
  ok.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([Remote]) ->
  AChild = {riak_repl2_rtsource_conn_mgr, {riak_repl2_rtsource_conn_mgr, start_link, [[Remote, self()]]},
    permanent, ?SHUTDOWN, worker, [riak_repl2_rtsource_conn_mgr]},

  {ok, {{one_for_one, 10, 10}, [AChild]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================