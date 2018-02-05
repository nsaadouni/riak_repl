-module(riak_repl2_rtsource_remote_conn_sup).
-author("nordine saadouni").

-behaviour(supervisor).

%% API
-export([start_link/1,
  add_connection/6,
  remove_connection/1
]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).
-define(SHUTDOWN, 5000). % how long to give rtsource processes to persist queue/shutdown

-record(ep, {addr,                                 %% endpoint {IP, Port}
  primary :: boolean(),         %% is a primary connection
  nb_curr_connections = 0 :: counter(), %% number of current connections
  nb_success = 0 :: counter(),   %% total successfull connects on this ep
  nb_failures = 0 :: counter(),  %% total failed connects on this ep
  is_black_listed = false :: boolean(), %% true after a failed connection attempt
  backoff_delay=0 :: counter(),  %% incremented on each failure, reset to zero on success
  failures = orddict:new() :: orddict:orddict(), %% failure reasons
  last_fail_time :: erlang:timestamp(),          %% time of last failure since 1970
  next_try_secs :: counter()     %% time in seconds to next retry attempt
}).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link(Remote) ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, [Remote]).

% rtsource_conn_mgr callback
add_connection(Remote, Socket, Transport, Endpoint, Proto, _Props) ->
  lager:info("Adding a connection and starting rtsource_conn ~p", [Remote]),
  ChildSpec = make_child(Remote, Socket, Transport, Endpoint, Proto, _Props),
  supervisor:start_child(?MODULE, ChildSpec).

% rtsource_conn_mgr callback
%% need to check if this will work with stop, or if I need to code some more in rtsource_conn to
%% kill the process as well.
remove_connection(Pid) ->
  ok.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([Remote]) ->
  AChild = {rtsource_conn_mgr, {riak_repl2_rtsource_conn_mgr, start_link, [[Remote, ?MODULE]]},
    permanent, ?SHUTDOWN, worker, [riak_repl2_rtsource_conn_mgr]},
  {ok, {{one_for_one, 10, 10}, [AChild]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

make_child(Remote, Socket, Transport, Endpoint, Proto, _Props) ->
  {Endpoint#ep.addr, {riak_repl2_rtsource_conn, start_link, [Remote, Socket, Transport, Endpoint, Proto, _Props]},
    permanent, ?SHUTDOWN, worker, [riak_repl2_rtsource_conn]}.